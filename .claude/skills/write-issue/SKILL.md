---
name: write-issue
description: Write an issue description based on the current git diff with main
allowed-tools: Bash(git diff*), Bash(git log*), Skill
---

# Write Issue Description

Generate an issue description by analyzing the current changes against main. The issue should describe the work as if it has not been done yet.

## Steps

1. Get the git diff between the current working state and main:
   ```bash
   git diff main...HEAD
   ```
   If there are uncommitted changes, also run:
   ```bash
   git diff
   ```

2. Analyze the diff and reverse-engineer what problem the changes are solving. Write the issue as if proposing the work before it was implemented.

3. Format the issue using this structure:
   ```
   ## Title

   [A concise title describing the feature or fix]

   ## Problem

   [Describe the problem that needs to be solved. What is the current state? What is wrong or missing? Why does this matter?]

   ## Proposed Solution

   [Describe what should be done to solve the problem. Be specific about the approach but write it as a proposal, not as completed work.]
   ```

4. After writing the first draft, invoke the humanizer skill to remove AI-generated writing patterns:
   ```
   /humanizer
   ```
   Apply the humanizer's suggestions to polish the final description.

5. Present the final polished issue description to the user.
