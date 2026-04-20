This is an unordered, unstructured collection of notes, acts as the long-term memory, of me, not agents.
Maybe can also be beneficial to agents later, but currently human (me) is doing all the coding.
This is nothing like document for public.
May be useful later for being the knowledge base where serious documents are "extracted" from.
The first thing I want to note down is that (me) should not write those documents.
Based on past experiences, whenever I start to create some serious documents for my researching codebase, my coding progress diminishes and the project begins to be abandoned.

About the crates.
Code is grouped into multiple crates based on dependencies and features, not functions.
Currently I create four crates, one with all application-level dependencies (OTLP and Prometheus exporter, etc), one with TCP dependencies, one with cryptography dependencies, and the core `libbft` crate for all the other code without these kinds of dependencies.
The code is in `libbft` crate is not because it is "protocol implementation" code or what, just because it does not have special dependencies, and I want to put as many code as possible into `libbft` so readers do not need to frequently track call stacks across crates.
This is also why crate names should somewhat reflect their dependencies instead of their function scopes.
Currently I have not deployed any feature gate yet, but if I do that, I may also group the code affected by a feature gate into a dedicated crate.