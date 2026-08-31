-------------------------- MODULE membership_broken --------------------------
(* The deliberately broken variant required by the project plan: it drops the
   joint-phase dual-majority requirement and accepts a majority of EITHER
   configuration instead of BOTH. This is the single-phase-switch bug joint
   consensus exists to prevent — a majority of Old and a majority of New are
   not guaranteed to share a voter, so two disjoint groups can each grant a
   quorum to a DIFFERENT candidate in the same term.

   TLC is expected to find a counterexample here. If it does not, this file has
   stopped being a meaningful negative control and must be fixed. *)

EXTENDS Naturals, FiniteSets

CONSTANT Nodes, Old, New, Terms, Phase

VARIABLES votes, elected

Vars == <<votes, elected>>

Init == /\ votes = {}
        /\ elected = {}

HasVoted(t, v) == \E c \in Nodes : <<t, v, c>> \in votes

CastVote(t, v, c) == /\ ~HasVoted(t, v)
                      /\ votes' = votes \cup {<<t, v, c>>}
                      /\ UNCHANGED elected

VotesFor(t, c) == {v \in Nodes : <<t, v, c>> \in votes}

Majority(S, Config) == 2 * Cardinality(S) > Cardinality(Config)

\* BROKEN: OR instead of AND during the joint phase. This is the one-line change
\* that makes the whole protocol unsafe, and the point of this file is to prove
\* that TLC actually notices.
HasQuorum(t, c) ==
    LET V == VotesFor(t, c) IN
    CASE Phase = "old"   -> Majority(V \cap Old, Old)
      [] Phase = "new"   -> Majority(V \cap New, New)
      [] Phase = "joint" -> Majority(V \cap Old, Old) \/ Majority(V \cap New, New)

BecomeLeader(t, c) == /\ HasQuorum(t, c)
                       /\ elected' = elected \cup {<<t, c>>}
                       /\ UNCHANGED votes

Next == \/ \E t \in Terms, v \in Nodes, c \in Nodes : CastVote(t, v, c)
        \/ \E t \in Terms, c \in Nodes : BecomeLeader(t, c)

Spec == Init /\ [][Next]_Vars

NoTwoLeaders == \A t \in Terms : Cardinality({n \in Nodes : <<t, n>> \in elected}) <= 1

=============================================================================
