# Sonar parity harness

Compares Java unused-private rules (java:S1144 methods, java:S1068 fields) from a tree-sitter
prototype against SonarCloud's open issues on six Apache repos, pinned to the commit SonarCloud
last analyzed.

    scripts/dev/sonar-parity/run.sh

Last result on main (non-test) sources: S1144 46/46, S1068 196/196, no extras.
Test sources are not scored yet; SonarCloud reports these rules there for struts and fineract
but not for dubbo, for reasons not visible through the public API.
