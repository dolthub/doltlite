import Prolly.OrderedMap

namespace DoltLite.Prolly

structure ChunkState (A : Type) where
  done : List (List A) := []
  pending : List A := []

def chunkContents (s : ChunkState A) : List A := s.done.flatten ++ s.pending

def chunkPush (cut : List A → Bool) (s : ChunkState A) (a : A) : ChunkState A :=
  let pending := s.pending ++ [a]
  if cut pending then ⟨s.done ++ [pending], []⟩ else ⟨s.done, pending⟩

def chunkFeed (cut : List A → Bool) : ChunkState A → List A → ChunkState A
  | s, [] => s
  | s, a :: as => chunkFeed cut (chunkPush cut s a) as

def chunkFinish (s : ChunkState A) : List (List A) :=
  if s.pending.isEmpty then s.done else s.done ++ [s.pending]

def chunk (cut : List A → Bool) (xs : List A) : List (List A) :=
  chunkFinish (chunkFeed cut {} xs)

theorem chunkPush_contents (cut : List A → Bool) (s : ChunkState A) (a : A) :
    chunkContents (chunkPush cut s a) = chunkContents s ++ [a] := by
  simp only [chunkPush]
  split <;> simp [chunkContents, List.flatten_append, List.append_assoc]

theorem chunkFeed_contents (cut : List A → Bool) (s : ChunkState A) (xs : List A) :
    chunkContents (chunkFeed cut s xs) = chunkContents s ++ xs := by
  induction xs generalizing s with
  | nil => simp [chunkFeed]
  | cons a as ih =>
    simp [chunkFeed, ih, chunkPush_contents, List.append_assoc]

theorem chunkFinish_contents (s : ChunkState A) :
    (chunkFinish s).flatten = chunkContents s := by
  cases hp : s.pending with
  | nil => simp [chunkFinish, chunkContents, hp]
  | cons a as => simp [chunkFinish, chunkContents, hp, List.flatten_append]

theorem chunk_preserves_rows (cut : List A → Bool) (xs : List A) :
    (chunk cut xs).flatten = xs := by
  unfold chunk
  rw [chunkFinish_contents, chunkFeed_contents]
  rfl

theorem chunkFeed_append (cut : List A → Bool) (s : ChunkState A) (xs ys : List A) :
    chunkFeed cut s (xs ++ ys) = chunkFeed cut (chunkFeed cut s xs) ys := by
  induction xs generalizing s with
  | nil => rfl
  | cons a as ih => simpa [chunkFeed] using ih (chunkPush cut s a)

theorem chunk_batch_independent (cut : List A → Bool) (xs ys : List A) :
    chunkFinish (chunkFeed cut (chunkFeed cut {} xs) ys) = chunk cut (xs ++ ys) := by
  simp [chunk, chunkFeed_append]

def HealthyChunks (s : ChunkState A) : Prop := ∀ c ∈ s.done, c ≠ []

theorem chunkPush_healthy (cut : List A → Bool) (s : ChunkState A) (a : A)
    (h : HealthyChunks s) : HealthyChunks (chunkPush cut s a) := by
  simp only [chunkPush]
  split
  · intro c hc
    simp only [List.mem_append, List.mem_singleton] at hc
    rcases hc with hc | rfl
    · exact h c hc
    · simp
  · exact h

theorem chunkFeed_healthy (cut : List A → Bool) (s : ChunkState A) (xs : List A)
    (h : HealthyChunks s) : HealthyChunks (chunkFeed cut s xs) := by
  induction xs generalizing s with
  | nil => exact h
  | cons a as ih => exact ih (chunkPush cut s a) (chunkPush_healthy cut s a h)

theorem chunk_nonempty (cut : List A → Bool) (xs : List A) :
    ∀ c ∈ chunk cut xs, c ≠ [] := by
  have h := chunkFeed_healthy cut ({} : ChunkState A) xs (by simp [HealthyChunks])
  unfold chunk chunkFinish
  split
  · exact h
  · rename_i hcut
    intro c hc
    simp only [List.mem_append, List.mem_singleton] at hc
    rcases hc with hc | rfl
    · exact h c hc
    · intro he
      apply hcut
      simp [he]

theorem chunked_edits_lookup (cut : Rows V → Bool) (xs : Rows V)
    (es : List (Edit V)) (q : Int) :
    lookup q (chunk cut (applyEdits xs es)).flatten =
      replay (fun k => lookup k xs) es q := by
  rw [chunk_preserves_rows, lookup_applyEdits]

theorem chunked_edits_sorted (cut : Rows V → Bool) (xs : Rows V)
    (es : List (Edit V)) (h : Sorted xs) :
    Sorted (chunk cut (applyEdits xs es)).flatten := by
  rw [chunk_preserves_rows]
  exact sorted_applyEdits xs es h

theorem chunk_history_independent (cut : Rows V → Bool) (xs ys : Rows V)
    (hx : Sorted xs) (hy : Sorted ys) (h : ∀ k, lookup k xs = lookup k ys) :
    chunk cut xs = chunk cut ys := by
  rw [rows_ext_sorted xs ys hx hy h]

theorem edits_chunk_history_independent (cut : Rows V → Bool) (xs : Rows V)
    (hx : Sorted xs) (es fs : List (Edit V))
    (h : ∀ k, replay (fun q => lookup q xs) es k = replay (fun q => lookup q xs) fs k) :
    chunk cut (applyEdits xs es) = chunk cut (applyEdits xs fs) := by
  apply chunk_history_independent cut _ _ (sorted_applyEdits xs es hx) (sorted_applyEdits xs fs hx)
  intro k
  simpa only [lookup_applyEdits] using h k

theorem content_address_history_independent (address : List (Rows V) → H)
    (cut : Rows V → Bool) (xs : Rows V) (hx : Sorted xs) (es fs : List (Edit V))
    (h : ∀ k, replay (fun q => lookup q xs) es k = replay (fun q => lookup q xs) fs k) :
    address (chunk cut (applyEdits xs es)) = address (chunk cut (applyEdits xs fs)) := by
  rw [edits_chunk_history_independent cut xs hx es fs h]

end DoltLite.Prolly
