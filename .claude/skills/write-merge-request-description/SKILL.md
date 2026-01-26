---
name: write-merge-request-description
description: Write a merge request description based on the current git diff with main
allowed-tools: Bash(git diff*), Bash(git log*), Skill
---

# Write Merge Request Description

Generate a merge request description by analyzing the current changes against main.

## Steps

1. Get the git diff between the current working state and main:
   ```bash
   git diff main...HEAD
   ```
   If there are uncommitted changes, also run:
   ```bash
   git diff
   ```

2. Analyze the diff and answer the question: **"What does this merge request do and why?"**
   - Focus on the purpose and intent behind the changes
   - Describe the problem being solved or feature being added
   - Explain any notable implementation decisions
   - Keep it concise but complete

3. Format the description using this structure:
   ```
   ## What does this merge request do and why?

   [Your description here]
   ```

4. After writing the first draft, invoke the humanizer skill to remove AI-generated writing patterns:
   ```
   /humanizer
   ```
   Apply the humanizer's suggestions to polish the final description.

5. Present the final polished merge request description to the user.
