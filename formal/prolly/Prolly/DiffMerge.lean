import Prolly.OrderedMap

namespace DoltLite.Prolly

inductive Delta (V : Type) where
  | add (value : V)
  | erase (old : V)
  | modify (old value : V)
  deriving DecidableEq

def diffValue [DecidableEq V] : Option V → Option V → Option (Delta V)
  | none, none => none
  | none, some v => some (.add v)
  | some v, none => some (.erase v)
  | some a, some b => if a = b then none else some (.modify a b)

def applyDelta (old : Option V) : Option (Delta V) → Option V
  | none => old
  | some (.add v) => some v
  | some (.erase _) => none
  | some (.modify _ v) => some v

def reverseDelta : Delta V → Delta V
  | .add v => .erase v
  | .erase v => .add v
  | .modify a b => .modify b a

theorem diffValue_none_iff [DecidableEq V] (a b : Option V) :
    diffValue a b = none ↔ a = b := by
  cases a <;> cases b <;> simp [diffValue]

theorem apply_diffValue [DecidableEq V] (a b : Option V) :
    applyDelta a (diffValue a b) = b := by
  cases a with
  | none => cases b <;> rfl
  | some a =>
    cases b with
    | none => rfl
    | some b => by_cases h : a = b <;> simp_all [diffValue, applyDelta]

theorem reverse_diffValue [DecidableEq V] (a b : Option V) :
    (diffValue a b).map reverseDelta = diffValue b a := by
  cases a with
  | none => cases b <;> rfl
  | some a =>
    cases b with
    | none => rfl
    | some b => by_cases h : a = b <;> simp_all [diffValue, reverseDelta, eq_comm]

theorem reverseDelta_involution (d : Delta V) : reverseDelta (reverseDelta d) = d := by
  cases d <;> rfl

def diffMaps [DecidableEq V] (old new : Int → Option V) : Int → Option (Delta V) :=
  fun k => diffValue (old k) (new k)

theorem diffMaps_complete [DecidableEq V] (old new : Int → Option V) (k : Int) :
    diffMaps old new k = none ↔ old k = new k := diffValue_none_iff _ _

theorem diffMaps_reconstructs [DecidableEq V] (old new : Int → Option V) :
    (fun k => applyDelta (old k) (diffMaps old new k)) = new := by
  funext k
  exact apply_diffValue _ _

def mergeValue [DecidableEq V] (base ours theirs : Option V) : Option (Option V) :=
  if ours = theirs then some ours
  else if ours = base then some theirs
  else if theirs = base then some ours
  else none

theorem mergeValue_conflict_iff [DecidableEq V] (base ours theirs : Option V) :
    mergeValue base ours theirs = none ↔
      ours ≠ theirs ∧ ours ≠ base ∧ theirs ≠ base := by
  by_cases hot : ours = theirs <;> by_cases hob : ours = base <;>
    by_cases htb : theirs = base <;> simp_all [mergeValue]

theorem mergeValue_same [DecidableEq V] (base v : Option V) :
    mergeValue base v v = some v := by simp [mergeValue]

theorem mergeValue_left_unchanged [DecidableEq V] (base v : Option V) :
    mergeValue base base v = some v := by
  by_cases h : base = v <;> simp_all [mergeValue]

theorem mergeValue_right_unchanged [DecidableEq V] (base v : Option V) :
    mergeValue base v base = some v := by
  by_cases h : v = base <;> simp_all [mergeValue]

theorem mergeValue_symmetric [DecidableEq V] (base ours theirs : Option V) :
    mergeValue base ours theirs = mergeValue base theirs ours := by
  by_cases hot : ours = theirs <;> by_cases hob : ours = base <;>
    by_cases htb : theirs = base <;> simp_all [mergeValue, eq_comm]

theorem mergeValue_result_from_branch [DecidableEq V] (base ours theirs result : Option V)
    (h : mergeValue base ours theirs = some result) : result = ours ∨ result = theirs := by
  by_cases hot : ours = theirs <;> by_cases hob : ours = base <;>
    by_cases htb : theirs = base <;> simp_all [mergeValue]

theorem mergeValue_delete_unchanged [DecidableEq V] (base : Option V) :
    mergeValue base none base = some none := mergeValue_right_unchanged base none

theorem mergeValue_insert_conflict [DecidableEq V] (a b : V) (h : a ≠ b) :
    mergeValue none (some a) (some b) = none := by simp [mergeValue, h]

end DoltLite.Prolly
