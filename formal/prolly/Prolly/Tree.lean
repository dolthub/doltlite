import Prolly.OrderedMap

namespace DoltLite.Prolly

mutual
inductive Tree (V : Type) where
  | leaf (rows : Rows V)
  | branch (children : Forest V)

inductive Forest (V : Type) where
  | nil
  | cons (upper : Int) (child : Tree V) (rest : Forest V)
end

mutual
def entries : Tree V → Rows V
  | .leaf xs => xs
  | .branch fs => forestEntries fs

def forestEntries : Forest V → Rows V
  | .nil => []
  | .cons _ t ts => entries t ++ forestEntries ts
end

mutual
def treeLookup : Tree V → Int → Option V
  | .leaf xs, k => lookup k xs
  | .branch fs, k => forestLookup fs k

def forestLookup : Forest V → Int → Option V
  | .nil, _ => none
  | .cons hi t ts, k => if k ≤ hi then treeLookup t k else forestLookup ts k
end

mutual
inductive ValidTree : Tree V → Prop where
  | leaf (h : Sorted xs) : ValidTree (.leaf xs)
  | branch (h : ValidForest fs) (hne : fs ≠ .nil) : ValidTree (.branch fs)

inductive ValidForest : Forest V → Prop where
  | nil : ValidForest .nil
  | cons (child : ValidTree t) (rest : ValidForest ts)
      (bound : ∀ e ∈ entries t, e.1 ≤ hi)
      (fence : ∃ v, (hi, v) ∈ entries t)
      (following : ∀ e ∈ forestEntries ts, hi < e.1) :
      ValidForest (.cons hi t ts)
end


mutual
theorem treeLookup_correct : (t : Tree V) → ValidTree t → (k : Int) →
    treeLookup t k = lookup k (entries t)
  | .leaf _, .leaf _, _ => rfl
  | .branch fs, .branch h _, k => forestLookup_correct fs h k

theorem forestLookup_correct : (fs : Forest V) → ValidForest fs → (k : Int) →
    forestLookup fs k = lookup k (forestEntries fs)
  | .nil, .nil, _ => rfl
  | .cons hi t ts, .cons ht hs hb _ hf, k => by
    by_cases h : k ≤ hi
    · have hn : lookup k (forestEntries ts) = none := by
        apply lookup_none_of_keys
        intro e he
        have := hf e he
        omega
      simp only [forestLookup, h, ite_true, forestEntries, lookup_append, hn]
      rw [treeLookup_correct t ht k]
      cases lookup k (entries t) <;> rfl
    · have hn : lookup k (entries t) = none := by
        apply lookup_none_of_keys
        intro e he
        have := hb e he
        omega
      simp only [forestLookup, h, ite_false, forestEntries, lookup_append, hn]
      exact forestLookup_correct ts hs k
end

mutual
theorem entries_sorted : (t : Tree V) → ValidTree t → Sorted (entries t)
  | .leaf _, .leaf h => h
  | .branch fs, .branch h _ => forestEntries_sorted fs h

theorem forestEntries_sorted : (fs : Forest V) → ValidForest fs → Sorted (forestEntries fs)
  | .nil, .nil => by simp [forestEntries, Sorted]
  | .cons hi t ts, .cons ht hs hb _ hf => by
    apply List.pairwise_append.mpr
    refine ⟨entries_sorted t ht, forestEntries_sorted ts hs, ?_⟩
    intro a ha b hbm
    have := hb a ha
    have := hf b hbm
    omega
end

mutual
def treeCount : Tree V → Nat
  | .leaf xs => xs.length
  | .branch fs => forestCount fs

def forestCount : Forest V → Nat
  | .nil => 0
  | .cons _ t ts => treeCount t + forestCount ts
end

mutual
theorem treeCount_correct : (t : Tree V) → treeCount t = (entries t).length
  | .leaf _ => rfl
  | .branch fs => forestCount_correct fs

theorem forestCount_correct : (fs : Forest V) → forestCount fs = (forestEntries fs).length
  | .nil => rfl
  | .cons _ t ts => by
    simp [forestCount, forestEntries, treeCount_correct t, forestCount_correct ts]
end

theorem tree_edits_lookup (t : Tree V) (ht : ValidTree t) (es : List (Edit V)) (q : Int) :
    lookup q (applyEdits (entries t) es) = replay (treeLookup t) es q := by
  have h : (fun k => lookup k (entries t)) = treeLookup t := by
    funext k
    exact (treeLookup_correct t ht k).symm
  rw [lookup_applyEdits, h]

theorem tree_edits_sorted (t : Tree V) (ht : ValidTree t) (es : List (Edit V)) :
    Sorted (applyEdits (entries t) es) := sorted_applyEdits _ es (entries_sorted t ht)

end DoltLite.Prolly
