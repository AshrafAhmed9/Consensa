------------------------------ MODULE membership ------------------------------
(* Models one property of Raft joint-consensus membership changes: quorum
   intersection. For a FIXED phase (old, joint, or new), can two different
   candidates both collect a legitimate quorum for the same term?

   The property this must NOT trivially satisfy: a candidate becomes leader only
   by collecting votes from a majority of the phase's active configuration(s) —
   a majority of Old during "old", a majority of New during "new", and a majority
   of BOTH Old and New simultaneously during "joint". Each node casts at most one
   vote per term. That single-vote rule is what forces any two majorities of the
   SAME config to share a voter, which is the actual safety argument — it must
   hold as a consequence of the model, not be assumed by the invariant.

   Scope, stated plainly: this spec fixes Phase as a constant and does not model
   the old -> joint -> new transition itself, or whether a leader elected before
   a transition remains valid after it. An earlier version of this file modeled
   the transition and TLC correctly found that a leader elected pre-transition
   and a different leader elected post-transition (using votes cast fresh under
   the new phase) can coexist in the same term — a real finding, but a separate
   question from quorum intersection: it depends on how the reconfiguration entry
   itself gets committed, which this model does not represent. See README.md. *)

EXTENDS Naturals, FiniteSets

CONSTANT Nodes, Old, New, Terms, Phase

VARIABLES votes,   \* set of <<t, voter, candidate>> triples: voter's ballot in term t
          elected  \* set of <<t, candidate>> pairs: candidates who reached quorum in term t

Vars == <<votes, elected>>

Init == /\ votes = {}
        /\ elected = {}

HasVoted(t, v) == \E c \in Nodes : <<t, v, c>> \in votes

\* A node votes for at most one candidate per term — casting a second vote in the same
\* term for a different candidate is not a legal step, which is what prevents two
\* disjoint-looking vote sets from secretly sharing a double-voting node.
CastVote(t, v, c) == /\ ~HasVoted(t, v)
                      /\ votes' = votes \cup {<<t, v, c>>}
                      /\ UNCHANGED elected

VotesFor(t, c) == {v \in Nodes : <<t, v, c>> \in votes}

Majority(S, Config) == 2 * Cardinality(S) > Cardinality(Config)

\* The actual joint-consensus rule: BOTH configurations must independently grant a
\* majority to the same candidate in the same term. This is disjoint-majority
\* election safety, and it is the line the broken variant deletes.
HasQuorum(t, c) ==
    LET V == VotesFor(t, c) IN
    CASE Phase = "old"   -> Majority(V \cap Old, Old)
      [] Phase = "new"   -> Majority(V \cap New, New)
      [] Phase = "joint" -> Majority(V \cap Old, Old) /\ Majority(V \cap New, New)

BecomeLeader(t, c) == /\ HasQuorum(t, c)
                       /\ elected' = elected \cup {<<t, c>>}
                       /\ UNCHANGED votes

Next == \/ \E t \in Terms, v \in Nodes, c \in Nodes : CastVote(t, v, c)
        \/ \E t \in Terms, c \in Nodes : BecomeLeader(t, c)

Spec == Init /\ [][Next]_Vars

NoTwoLeaders == \A t \in Terms : Cardinality({n \in Nodes : <<t, n>> \in elected}) <= 1

=============================================================================
