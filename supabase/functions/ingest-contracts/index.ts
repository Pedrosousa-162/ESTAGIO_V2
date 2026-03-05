/**
 * Edge Function: ingest-contracts
 *
 * Ingere contratos celebrados a partir do endpoint GetInfoContrato da API BASE.
 * Segue o mesmo padrão de ingest-base: fetch → map → hash → dedup → batch upsert.
 *
 * Request body:
 *   {
 *     from_date?:  string,   // YYYY-MM-DD, default: hoje - 7 dias
 *     to_date?:    string,   // YYYY-MM-DD, default: hoje
 *     tenant_id?:  string,   // default: primeiro tenant
 *     dry_run?:    boolean   // se true, não persiste nada
 *   }
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { listAllContracts, mapToContract, parseNifNome } from "../_shared/baseApi.ts";
import { computeHash } from "../_shared/canonicalJson.ts";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Content-Type": "application/json",
};

const BATCH_SIZE = 200;

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });

  const startedAt = Date.now();

  try {
    const body = req.method === "POST"
      ? await req.json().catch(() => ({}))
      : {};

    const today = new Date();
    const minus7 = new Date(today);
    minus7.setDate(minus7.getDate() - 7);

    const fromDate: string = body.from_date ?? isoDate(minus7);
    const toDate: string = body.to_date ?? isoDate(today);
    const dryRun: boolean = body.dry_run === true;

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
      { auth: { autoRefreshToken: false, persistSession: false } },
    );

    // Resolve tenant_id
    let tenantId: string = body.tenant_id ?? "";
    if (!tenantId) {
      const { data: tenant, error } = await supabase
        .from("tenants").select("id").limit(1).single();
      if (error || !tenant) {
        return new Response(
          JSON.stringify({ error: "No tenant found. Run admin-seed first." }),
          { status: 400, headers: CORS },
        );
      }
      tenantId = tenant.id;
    }

    console.log(`[ingest-contracts] tenant=${tenantId} from=${fromDate} to=${toDate} dry_run=${dryRun}`);

    // 1. Fetch from BASE API
    const rawItems = await listAllContracts(fromDate, toDate);
    console.log(`[ingest-contracts] fetched ${rawItems.length} items`);

    const stats = {
      fetched: rawItems.length,
      inserted: 0,
      updated: 0,
      skipped: 0,
      linked_to_announcements: 0,
      entities_touched: 0,
      companies_touched: 0,
      errors: 0,
      dry_run: dryRun,
      elapsed_ms: 0,
    };

    if (dryRun || rawItems.length === 0) {
      if (dryRun) stats.inserted = rawItems.length;
      stats.elapsed_ms = Date.now() - startedAt;
      return new Response(JSON.stringify(stats), { status: 200, headers: CORS });
    }

    // 2. Map + hash all items
    const mapped = await Promise.all(
      rawItems.map(async (raw) => {
        const contract = mapToContract(raw as Record<string, unknown>);
        const hash = await computeHash(contract.raw_payload, ["updated_at", "created_at", "raw_hash"]);
        return { contract, hash };
      }),
    );

    // 3. Load existing hashes in bulk for deduplication
    const baseContractIds = mapped
      .map((m) => m.contract.base_contract_id)
      .filter(Boolean) as string[];

    const existingMap = new Map<string, { id: string; hash: string }>();

    for (let i = 0; i < baseContractIds.length; i += 500) {
      const chunk = baseContractIds.slice(i, i + 500);
      const { data } = await supabase
        .from("contracts")
        .select("id, base_contract_id, raw_hash")
        .eq("tenant_id", tenantId)
        .in("base_contract_id", chunk);
      (data ?? []).forEach((row: { id: string; base_contract_id: string; raw_hash: string }) => {
        existingMap.set(row.base_contract_id, { id: row.id, hash: row.raw_hash });
      });
    }

    // 4. Split into new vs changed
    const toInsert: typeof mapped = [];
    const toUpdate: typeof mapped = [];

    for (const item of mapped) {
      const existing = item.contract.base_contract_id
        ? existingMap.get(item.contract.base_contract_id)
        : undefined;

      if (!existing) {
        toInsert.push(item);
      } else if (existing.hash !== item.hash) {
        toUpdate.push(item);
      } else {
        stats.skipped++;
      }
    }

    // 5. Pre-load announcements for linking (by nAnuncio)
    const announcementNos = mapped
      .map((m) => m.contract.base_announcement_no)
      .filter(Boolean) as string[];

    const announcementMap = new Map<string, string>(); // announcement_no → announcement_id

    if (announcementNos.length > 0) {
      for (let i = 0; i < announcementNos.length; i += 500) {
        const chunk = announcementNos.slice(i, i + 500);
        const { data } = await supabase
          .from("announcements")
          .select("id, dr_announcement_no")
          .eq("tenant_id", tenantId)
          .in("dr_announcement_no", chunk);
        (data ?? []).forEach((row: { id: string; dr_announcement_no: string }) => {
          if (row.dr_announcement_no) {
            announcementMap.set(row.dr_announcement_no, row.id);
          }
        });
      }
    }

    // 6. Pre-load/create entities and companies for linking
    const entityNifs = new Set<string>();
    const companyNifs = new Set<string>();

    for (const { contract } of mapped) {
      for (const raw of contract.contracting_entities) {
        const { nif } = parseNifNome(raw);
        if (nif) entityNifs.add(nif);
      }
      for (const raw of contract.winners) {
        const { nif } = parseNifNome(raw);
        if (nif) companyNifs.add(nif);
      }
    }

    // Load existing entities
    const entityMap = new Map<string, string>(); // nif → entity_id
    if (entityNifs.size > 0) {
      const nifArr = [...entityNifs];
      for (let i = 0; i < nifArr.length; i += 500) {
        const chunk = nifArr.slice(i, i + 500);
        const { data } = await supabase
          .from("entities")
          .select("id, nif")
          .eq("tenant_id", tenantId)
          .in("nif", chunk);
        (data ?? []).forEach((row: { id: string; nif: string }) => {
          entityMap.set(row.nif, row.id);
        });
      }

      // Create missing entities (basic records to be enriched later by extract-entities)
      const missingEntityNifs = nifArr.filter((nif) => !entityMap.has(nif));
      if (missingEntityNifs.length > 0) {
        // Build a name lookup from the contracts
        const nifNameMap = new Map<string, string>();
        for (const { contract } of mapped) {
          for (const raw of contract.contracting_entities) {
            const { nif, name } = parseNifNome(raw);
            if (nif && name) nifNameMap.set(nif, name);
          }
        }

        const entityRows = missingEntityNifs.map((nif) => ({
          tenant_id: tenantId,
          nif,
          name: nifNameMap.get(nif) ?? nif,
        }));

        for (let i = 0; i < entityRows.length; i += BATCH_SIZE) {
          const batch = entityRows.slice(i, i + BATCH_SIZE);
          const { data, error } = await supabase
            .from("entities")
            .upsert(batch, { onConflict: "tenant_id,nif", ignoreDuplicates: true })
            .select("id, nif");
          if (!error && data) {
            data.forEach((row: { id: string; nif: string }) => {
              entityMap.set(row.nif, row.id);
            });
            stats.entities_touched += data.length;
          }
        }
      }
    }

    // Load existing companies
    const companyMap = new Map<string, string>(); // nif → company_id
    if (companyNifs.size > 0) {
      const nifArr = [...companyNifs];
      for (let i = 0; i < nifArr.length; i += 500) {
        const chunk = nifArr.slice(i, i + 500);
        const { data } = await supabase
          .from("companies")
          .select("id, nif")
          .eq("tenant_id", tenantId)
          .in("nif", chunk);
        (data ?? []).forEach((row: { id: string; nif: string }) => {
          companyMap.set(row.nif, row.id);
        });
      }

      // Create missing companies
      const missingCompanyNifs = nifArr.filter((nif) => !companyMap.has(nif));
      if (missingCompanyNifs.length > 0) {
        const nifNameMap = new Map<string, string>();
        for (const { contract } of mapped) {
          for (const raw of contract.winners) {
            const { nif, name } = parseNifNome(raw);
            if (nif && name) nifNameMap.set(nif, name);
          }
        }

        const companyRows = missingCompanyNifs.map((nif) => ({
          tenant_id: tenantId,
          nif,
          name: nifNameMap.get(nif) ?? nif,
        }));

        for (let i = 0; i < companyRows.length; i += BATCH_SIZE) {
          const batch = companyRows.slice(i, i + BATCH_SIZE);
          const { data, error } = await supabase
            .from("companies")
            .upsert(batch, { onConflict: "tenant_id,nif", ignoreDuplicates: true })
            .select("id, nif");
          if (!error && data) {
            data.forEach((row: { id: string; nif: string }) => {
              companyMap.set(row.nif, row.id);
            });
            stats.companies_touched += data.length;
          }
        }
      }
    }

    // Helper: resolve entity_id and winner_company_id for a contract
    function resolveLinks(contract: ReturnType<typeof mapToContract>) {
      let announcementId: string | null = null;
      let entityId: string | null = null;
      let winnerCompanyId: string | null = null;

      if (contract.base_announcement_no) {
        announcementId = announcementMap.get(contract.base_announcement_no) ?? null;
      }

      // Use first contracting entity
      if (contract.contracting_entities.length > 0) {
        const { nif } = parseNifNome(contract.contracting_entities[0]);
        entityId = entityMap.get(nif) ?? null;
      }

      // Use first winner
      if (contract.winners.length > 0) {
        const { nif } = parseNifNome(contract.winners[0]);
        winnerCompanyId = companyMap.get(nif) ?? null;
      }

      return { announcementId, entityId, winnerCompanyId };
    }

    // 7. Batch insert new contracts
    for (let i = 0; i < toInsert.length; i += BATCH_SIZE) {
      const batch = toInsert.slice(i, i + BATCH_SIZE).map(({ contract, hash }) => {
        const { announcementId, entityId, winnerCompanyId } = resolveLinks(contract);
        if (announcementId) stats.linked_to_announcements++;

        return {
          tenant_id: tenantId,
          source: "BASE_API",
          base_contract_id: contract.base_contract_id,
          base_procedure_id: contract.base_procedure_id,
          base_announcement_no: contract.base_announcement_no,
          base_incm_id: contract.base_incm_id,
          announcement_id: announcementId,
          object: contract.object,
          description: contract.description,
          procedure_type: contract.procedure_type,
          contract_type: contract.contract_type,
          announcement_type: contract.announcement_type,
          legal_regime: contract.legal_regime,
          legal_basis: contract.legal_basis,
          publication_date: contract.publication_date,
          award_date: contract.award_date,
          signing_date: contract.signing_date,
          close_date: contract.close_date,
          base_price: contract.base_price,
          contract_price: contract.contract_price,
          effective_price: contract.effective_price,
          currency: contract.currency,
          contracting_entities: contract.contracting_entities,
          winners: contract.winners,
          competitors: contract.competitors,
          cpv_main: contract.cpv_main,
          cpv_list: contract.cpv_list,
          execution_deadline_days: contract.execution_deadline_days,
          execution_locations: contract.execution_locations,
          framework_agreement: contract.framework_agreement,
          is_centralized: contract.is_centralized,
          is_ecological: contract.is_ecological,
          end_type: contract.end_type,
          procedure_docs_url: contract.procedure_docs_url,
          observations: contract.observations,
          entity_id: entityId,
          winner_company_id: winnerCompanyId,
          raw_payload: contract.raw_payload,
          raw_hash: hash,
        };
      });

      const { error } = await supabase
        .from("contracts")
        .insert(batch);
      if (error) {
        console.error("[ingest-contracts] insert batch error:", error.message, error.details, error.hint, error.code);
        // Log a sample row for debugging
        if (batch.length > 0) {
          const sample = batch[0];
          console.error("[ingest-contracts] sample row base_contract_id:", sample.base_contract_id, "cpv_main:", sample.cpv_main, "pub_date:", sample.publication_date);
        }
        stats.errors += batch.length;
      } else {
        stats.inserted += batch.length;
      }
    }

    // 8. Update changed contracts
    for (const { contract, hash } of toUpdate) {
      const existing = existingMap.get(contract.base_contract_id!);
      if (!existing) continue;

      const { announcementId, entityId, winnerCompanyId } = resolveLinks(contract);

      const { error } = await supabase
        .from("contracts")
        .update({
          object: contract.object,
          description: contract.description,
          procedure_type: contract.procedure_type,
          contract_type: contract.contract_type,
          announcement_type: contract.announcement_type,
          legal_regime: contract.legal_regime,
          legal_basis: contract.legal_basis,
          publication_date: contract.publication_date,
          award_date: contract.award_date,
          signing_date: contract.signing_date,
          close_date: contract.close_date,
          base_price: contract.base_price,
          contract_price: contract.contract_price,
          effective_price: contract.effective_price,
          contracting_entities: contract.contracting_entities,
          winners: contract.winners,
          competitors: contract.competitors,
          cpv_main: contract.cpv_main,
          cpv_list: contract.cpv_list,
          execution_deadline_days: contract.execution_deadline_days,
          execution_locations: contract.execution_locations,
          framework_agreement: contract.framework_agreement,
          is_centralized: contract.is_centralized,
          is_ecological: contract.is_ecological,
          end_type: contract.end_type,
          procedure_docs_url: contract.procedure_docs_url,
          observations: contract.observations,
          announcement_id: announcementId,
          entity_id: entityId,
          winner_company_id: winnerCompanyId,
          raw_payload: contract.raw_payload,
          raw_hash: hash,
        })
        .eq("id", existing.id);

      if (error) {
        stats.errors++;
      } else {
        stats.updated++;
      }
    }

    stats.elapsed_ms = Date.now() - startedAt;
    console.log("[ingest-contracts] done:", stats);

    return new Response(JSON.stringify(stats), { status: 200, headers: CORS });
  } catch (err) {
    console.error("[ingest-contracts] fatal:", err);
    return new Response(
      JSON.stringify({ error: String(err) }),
      { status: 500, headers: CORS },
    );
  }
});
