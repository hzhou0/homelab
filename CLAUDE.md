# Claude conventions

## Comment style

Comments explain **why**, not what. The code and key names already say what.

Keep:
- Non-obvious constraints or invariants (e.g. "empty `{}` also matches pods")
- Design decisions that aren't derivable from the code (e.g. why a native VAP instead of Kyverno)
- Ordering dependencies or subtle interactions between resources

Remove:
- Prose restating what the resource type or field name already says
- Step-by-step operational commands (those belong in the README)
- Cross-references to the README or design doc (the reader can find those)

Same rule applies to README files: architecture and rationale stay; shell command sequences and
migration procedures go.
