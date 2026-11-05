import Prolly.Tree
import Prolly.Chunker
import Prolly.DiffMerge

namespace DoltLite.Prolly

def fenceTree : Tree Nat :=
  .branch (.cons 0 (.leaf [(-1, 10), (0, 11)]) (.cons 5 (.leaf [(5, 12)]) .nil))

theorem fenceTree_valid : ValidTree fenceTree := by
  apply ValidTree.branch
  · apply ValidForest.cons (.leaf (by unfold Sorted; decide))
    · apply ValidForest.cons (.leaf (by unfold Sorted; decide)) .nil
      · intro e he
        simp only [entries, List.mem_singleton] at he
        subst e
        decide
      · exact ⟨12, by simp [entries]⟩
      · simp [forestEntries]
    · intro e he
      simp [entries] at he
      rcases he with rfl | rfl <;> decide
    · exact ⟨11, by simp [entries]⟩
    · intro e he
      simp [forestEntries, entries] at he
      subst e
      decide
  · intro h
    cases h

theorem fence_lookup_example : treeLookup fenceTree 0 = some 11 := by decide

theorem gap_lookup_example : treeLookup fenceTree 2 = none := by decide

theorem invalid_fence_rejected :
    ¬ ValidTree (.branch (.cons 0 (.leaf [(5, 12)]) .nil) : Tree Nat) := by
  intro h
  cases h with
  | branch hf _ =>
    cases hf with
    | cons _ _ _ fence _ =>
      obtain ⟨v, hv⟩ := fence
      simp [entries] at hv

theorem deletion_merge_example : mergeValue (some 4) none (some 4) = some none := by decide

theorem delete_modify_conflict_example : mergeValue (some 4) none (some 5) = none := by decide

end DoltLite.Prolly
