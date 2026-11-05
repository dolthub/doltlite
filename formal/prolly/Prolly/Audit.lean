import Prolly.Examples
import Lean

open Lean Elab Command in
run_cmd do
  let env ← getEnv
  let allowed : Array Name := #[`propext, `Classical.choice, `Quot.sound]
  let required : Array Name := #[
    `DoltLite.Prolly.lookup_applyEdits,
    `DoltLite.Prolly.rows_ext_sorted,
    `DoltLite.Prolly.treeLookup_correct,
    `DoltLite.Prolly.treeCount_correct,
    `DoltLite.Prolly.chunk_preserves_rows,
    `DoltLite.Prolly.chunk_batch_independent,
    `DoltLite.Prolly.content_address_history_independent,
    `DoltLite.Prolly.diffMaps_reconstructs,
    `DoltLite.Prolly.mergeValue_conflict_iff,
    `DoltLite.Prolly.fenceTree_valid]
  for name in required do
    unless (env.find? name).any (·.isTheorem) do
      throwError "Missing required Prolly theorem: {name}"
  let mut count : Nat := 0
  for (name, info) in env.constants do
    if (`DoltLite.Prolly).isPrefixOf name then
      for ax in ← collectAxioms name do
        unless allowed.contains ax do
          throwError "Prolly declaration {name} depends on forbidden axiom {ax}"
      if info.isTheorem then count := count + 1
  logInfo m!"Audited {count} Prolly theorems; only standard Lean axioms are permitted."
