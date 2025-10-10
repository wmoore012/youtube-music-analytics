# License Audit Report (MIT standardization)

Date: 2025-10-10

## Summary
- Root LICENSE file declares the project under the MIT License.
- Four files in the web/ directory include GPL-3.0-or-later SPDX headers.

## Affected files
- web/etl_helpers.py
- web/youtube_integration.py
- web/youtube_version_parser.py
- web/youtube_metrics_helpers.py

## Impact
- Creates legal ambiguity: those specific files are marked as GPL while the project declares MIT at the root.
- This can confuse community users, recruiters, and collaborators, and complicate reuse or redistribution.

## Recommendation
- Standardize the entire repository on the MIT License by removing the GPL-3.0-or-later SPDX headers from the 4 web/ files listed above.
- Optionally, you may add `# SPDX-License-Identifier: MIT` file headers, but it is also acceptable to rely on the root LICENSE.

## Context
This project is intended as an open-source gift to the community from a developer new to open source. Aligning all files to MIT avoids licensing confusion and makes reuse straightforward.

## Verification steps (post-fix)
- Search for GPL headers:
  - `grep -R --line-number "SPDX-License-Identifier: GPL-3.0-or-later" web/`  (expect no results)
- Confirm root LICENSE remains MIT.

