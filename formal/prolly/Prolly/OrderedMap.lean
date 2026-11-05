import Std

namespace DoltLite.Prolly

abbrev Rows (V : Type) := List (Int × V)

def lookup (k : Int) : Rows V → Option V
  | [] => none
  | e :: es => if k = e.1 then some e.2 else lookup k es

def Sorted (xs : Rows V) : Prop := xs.Pairwise (fun a b => a.1 < b.1)

def put (k : Int) (v : V) : Rows V → Rows V
  | [] => [(k, v)]
  | e :: es =>
    if k < e.1 then (k, v) :: e :: es
    else if k = e.1 then (k, v) :: es
    else e :: put k v es

def erase (k : Int) : Rows V → Rows V
  | [] => []
  | e :: es => if k = e.1 then erase k es else e :: erase k es

theorem lookup_put (xs : Rows V) (k q : Int) (v : V) :
    lookup q (put k v xs) = if q = k then some v else lookup q xs := by
  induction xs with
  | nil => simp [put, lookup]
  | cons e es ih =>
    simp only [put]
    split
    · by_cases hq : q = k <;> simp_all [lookup]
    · split
      · by_cases hq : q = k <;> simp_all [lookup]
      · by_cases hq : q = k <;> by_cases hqe : q = e.1 <;>
          simp_all [lookup]

theorem lookup_erase (xs : Rows V) (k q : Int) :
    lookup q (erase k xs) = if q = k then none else lookup q xs := by
  induction xs with
  | nil => simp [erase, lookup]
  | cons e es ih =>
    by_cases heq : k = e.1 <;> by_cases hq : q = k <;>
      by_cases hqe : q = e.1 <;> simp_all [erase, lookup]

theorem mem_put_subset (xs : Rows V) (k : Int) (v : V) (e : Int × V)
    (h : e ∈ put k v xs) : e = (k, v) ∨ e ∈ xs := by
  induction xs with
  | nil => simpa [put] using h
  | cons a es ih =>
    simp only [put] at h
    split at h
    · simpa only [List.mem_cons, or_assoc] using h
    · split at h
      · rcases List.mem_cons.mp h with h | h
        · exact Or.inl h
        · exact Or.inr (List.mem_cons_of_mem a h)
      · rcases List.mem_cons.mp h with h | h
        · exact Or.inr (List.mem_cons.mpr (Or.inl h))
        · rcases ih h with h | h
          · exact Or.inl h
          · exact Or.inr (List.mem_cons_of_mem a h)

theorem mem_erase_subset (xs : Rows V) (k : Int) (e : Int × V)
    (h : e ∈ erase k xs) : e ∈ xs := by
  induction xs with
  | nil => simp [erase] at h
  | cons a es ih =>
    simp only [erase] at h
    split at h
    · exact List.mem_cons_of_mem a (ih h)
    · rcases List.mem_cons.mp h with h | h
      · exact List.mem_cons.mpr (Or.inl h)
      · exact List.mem_cons_of_mem a (ih h)

theorem sorted_put (xs : Rows V) (k : Int) (v : V) (h : Sorted xs) :
    Sorted (put k v xs) := by
  induction xs with
  | nil => simp [put, Sorted]
  | cons a es ih =>
    obtain ⟨ha, ht⟩ := List.pairwise_cons.mp h
    simp only [put]
    split
    · rename_i hlt
      apply List.pairwise_cons.mpr
      refine ⟨?_, h⟩
      intro e he
      rcases List.mem_cons.mp he with rfl | he
      · exact hlt
      · exact Int.lt_trans hlt (ha e he)
    · rename_i hlt
      split
      · rename_i heq
        exact List.pairwise_cons.mpr ⟨by simpa only [heq] using ha, ht⟩
      · rename_i heq
        apply List.pairwise_cons.mpr
        refine ⟨?_, ih ht⟩
        intro e he
        rcases mem_put_subset es k v e he with rfl | he
        · dsimp
          omega
        · exact ha e he

theorem sorted_erase (xs : Rows V) (k : Int) (h : Sorted xs) :
    Sorted (erase k xs) := by
  induction xs with
  | nil => simp [erase, Sorted]
  | cons a es ih =>
    obtain ⟨ha, ht⟩ := List.pairwise_cons.mp h
    by_cases heq : k = a.1
    · simpa [erase, heq] using ih ht
    · simp only [erase, heq, ite_false]
      exact List.pairwise_cons.mpr
        ⟨fun e he => ha e (mem_erase_subset es k e he), ih ht⟩

inductive Edit (V : Type) where
  | put (key : Int) (value : V)
  | erase (key : Int)

def applyEdit (xs : Rows V) : Edit V → Rows V
  | .put k v => put k v xs
  | .erase k => erase k xs

def applyEdits : Rows V → List (Edit V) → Rows V
  | xs, [] => xs
  | xs, e :: es => applyEdits (applyEdit xs e) es

def update (m : Int → Option V) : Edit V → Int → Option V
  | .put k v, q => if q = k then some v else m q
  | .erase k, q => if q = k then none else m q

def replay : (Int → Option V) → List (Edit V) → Int → Option V
  | m, [] => m
  | m, e :: es => replay (update m e) es

theorem lookup_applyEdit (xs : Rows V) (e : Edit V) (q : Int) :
    lookup q (applyEdit xs e) = update (fun k => lookup k xs) e q := by
  cases e <;> simp [applyEdit, update, lookup_put, lookup_erase]

theorem lookup_applyEdits (xs : Rows V) (es : List (Edit V)) (q : Int) :
    lookup q (applyEdits xs es) = replay (fun k => lookup k xs) es q := by
  induction es generalizing xs with
  | nil => rfl
  | cons e es ih =>
    have h : (fun k => lookup k (applyEdit xs e)) = update (fun k => lookup k xs) e := by
      funext k
      exact lookup_applyEdit xs e k
    simpa [applyEdits, replay, h] using ih (applyEdit xs e)

theorem sorted_applyEdits (xs : Rows V) (es : List (Edit V)) (h : Sorted xs) :
    Sorted (applyEdits xs es) := by
  induction es generalizing xs with
  | nil => exact h
  | cons e es ih =>
    apply ih
    cases e with
    | put k v => exact sorted_put xs k v h
    | erase k => exact sorted_erase xs k h

theorem put_overwrite (xs : Rows V) (k q : Int) (a b : V) :
    lookup q (put k b (put k a xs)) = lookup q (put k b xs) := by
  simp only [lookup_put]
  split <;> simp_all

theorem erase_after_put (xs : Rows V) (k q : Int) (v : V) :
    lookup q (erase k (put k v xs)) = lookup q (erase k xs) := by
  simp only [lookup_put, lookup_erase]
  split <;> simp_all

theorem independent_puts_commute (xs : Rows V) (k j q : Int) (v w : V) (h : k ≠ j) :
    lookup q (put k v (put j w xs)) = lookup q (put j w (put k v xs)) := by
  by_cases hk : q = k <;> by_cases hj : q = j <;> simp_all [lookup_put]

theorem lookup_append (xs ys : Rows V) (k : Int) :
    lookup k (xs ++ ys) =
      match lookup k xs with
      | none => lookup k ys
      | some v => some v := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
    by_cases h : k = a.1 <;> simp [lookup, h, ih]

theorem lookup_none_of_keys (xs : Rows V) (k : Int)
    (h : ∀ e ∈ xs, k ≠ e.1) : lookup k xs = none := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
    have ha := h a (by simp)
    have ht : ∀ e ∈ xs, k ≠ e.1 := fun e he => h e (by simp [he])
    simpa [lookup, ha] using ih ht

theorem lookup_some_mem (xs : Rows V) (k : Int) (v : V)
    (h : lookup k xs = some v) : (k, v) ∈ xs := by
  induction xs with
  | nil => simp [lookup] at h
  | cons a xs ih =>
    by_cases hk : k = a.1
    · have hv : a.2 = v := Option.some.inj (by simpa only [lookup, hk, ite_true] using h)
      exact List.mem_cons.mpr (Or.inl (Prod.ext hk hv.symm))
    · simp only [lookup, hk, ite_false] at h
      exact List.mem_cons_of_mem a (ih h)

theorem head_le_of_lookup (a : Int × V) (xs : Rows V) (k : Int) (v : V)
    (hs : Sorted (a :: xs)) (h : lookup k (a :: xs) = some v) : a.1 ≤ k := by
  rcases List.mem_cons.mp (lookup_some_mem _ k v h) with he | he
  · have := congrArg Prod.fst he
    dsimp at this
    omega
  · exact Int.le_of_lt ((List.pairwise_cons.mp hs).1 (k, v) he)

theorem rows_ext_sorted (xs ys : Rows V) (hx : Sorted xs) (hy : Sorted ys)
    (h : ∀ k, lookup k xs = lookup k ys) : xs = ys := by
  induction xs generalizing ys with
  | nil =>
    cases ys with
    | nil => rfl
    | cons b bs => have he := h b.1; simp [lookup] at he
  | cons a xs ih =>
    cases ys with
    | nil => have he := h a.1; simp [lookup] at he
    | cons b bs =>
      have ha : lookup a.1 (b :: bs) = some a.2 := by
        simpa only [lookup, ite_true] using (h a.1).symm
      have hb : lookup b.1 (a :: xs) = some b.2 := by
        simpa only [lookup, ite_true] using h b.1
      have hle := head_le_of_lookup a xs b.1 b.2 hx hb
      have hge := head_le_of_lookup b bs a.1 a.2 hy ha
      have hk : a.1 = b.1 := by omega
      have hv : a.2 = b.2 := Option.some.inj (by simpa only [lookup, hk, ite_true] using h a.1)
      have hab : a = b := Prod.ext hk hv
      subst b
      obtain ⟨hax, hxs⟩ := List.pairwise_cons.mp hx
      obtain ⟨hay, hys⟩ := List.pairwise_cons.mp hy
      congr 1
      apply ih bs hxs hys
      intro k
      by_cases he : k = a.1
      · subst k
        have hn : lookup a.1 xs = none := lookup_none_of_keys xs a.1 (by
          intro e he
          have := hax e he
          omega)
        have hm : lookup a.1 bs = none := lookup_none_of_keys bs a.1 (by
          intro e he
          have := hay e he
          omega)
        rw [hn, hm]
      · simpa only [lookup, he, ite_false] using h k

end DoltLite.Prolly
