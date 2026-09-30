# Both-specification requirement audit

This is an in-progress acceptance audit, not a completion claim. The latest recovered implementation is preserved in GitHub. Missing-source statements from the interrupted run have been replaced with current evidence.

The exact source requirements are in the two files below. `SPECIFICATION_AUDIT.json` retains every section's full text and contiguous source-line range, implementation state, verification state and evidence. `HYBRID_COMPLETION_CHECKLIST.csv` tracks all 184 exact checkboxes individually. A completed offline check does not imply live service verification.

Current checkbox counts: {"COMPLETED":119,"FAILED":62,"BLOCKED BY ENVIRONMENT":3}.
Current section counts: {"BLOCKED BY ENVIRONMENT":9,"COMPLETED":56,"FAILED":14}.

| Specification section | Lines | Status | Evidence group |
|---|---:|---|---|
| ORIGINAL-01: Hybrid TikTok Scanner Backend + Full Performance Optimization | 1–49 | BLOCKED BY ENVIRONMENT | live |
| ORIGINAL-02: Architecture | 50–76 | COMPLETED | review |
| ORIGINAL-03: Existing Worker Backend | 77–92 | COMPLETED | protocol |
| ORIGINAL-04: Direct TikTok Backend | 93–128 | COMPLETED | protocol |
| ORIGINAL-05: Unified Profile Format | 129–162 | COMPLETED | protocol |
| ORIGINAL-06: Unified Followers / Following Format | 163–186 | COMPLETED | protocol |
| ORIGINAL-07: Add Missing Evil Fields | 187–215 | COMPLETED | protocol |
| ORIGINAL-08: Backend Modes | 216–246 | COMPLETED | config |
| ORIGINAL-09: Hybrid Dispatcher | 247–283 | COMPLETED | routing |
| ORIGINAL-10: Dynamic Routing | 284–312 | COMPLETED | routing |
| ORIGINAL-11: Backend Scoring | 313–354 | COMPLETED | routing |
| ORIGINAL-12: Cursor-Chain Ownership | 355–382 | COMPLETED | routing |
| ORIGINAL-13: Cursor Failover | 383–399 | COMPLETED | routing |
| ORIGINAL-14: Page Size | 400–422 | BLOCKED BY ENVIRONMENT | live |
| ORIGINAL-15: Parallel Account Pagination | 423–446 | COMPLETED | routing |
| ORIGINAL-16: Worker Counts | 447–479 | COMPLETED | config |
| ORIGINAL-17: Connection Pooling | 480–495 | COMPLETED | connections |
| ORIGINAL-18: Direct Proxy Support | 496–531 | COMPLETED | connections |
| ORIGINAL-19: Adaptive Direct Concurrency | 532–571 | COMPLETED | routing |
| ORIGINAL-20: Retry Behaviour | 572–593 | COMPLETED | failures |
| ORIGINAL-21: Backend-Specific Failure States | 594–616 | COMPLETED | failures |
| ORIGINAL-22: Global 429 Behaviour | 617–639 | COMPLETED | rate |
| ORIGINAL-23: Optional Racing | 640–663 | COMPLETED | routing |
| ORIGINAL-24: Caching | 664–675 | COMPLETED | review |
| ORIGINAL-25: Database and Logging | 676–694 | COMPLETED | persistence |
| ORIGINAL-26: Statistics | 695–735 | COMPLETED | metrics |
| ORIGINAL-27: Console | 736–749 | COMPLETED | metrics |
| ORIGINAL-28: HTML Report | 750–759 | COMPLETED | report |
| ORIGINAL-29: Benchmark Before and After | 760–802 | FAILED | performance |
| ORIGINAL-30: 10,000 Follower Test | 803–833 | FAILED | performance |
| ORIGINAL-31: Large Scan Testing | 834–849 | FAILED | performance |
| ORIGINAL-32: Preserve Existing Scanner Behaviour | 850–869 | COMPLETED | failures |
| ORIGINAL-33: FINAL MANDATORY FULL-CODE REVIEW AND SPEED PASS | 870–1106 | COMPLETED | review |
| ORIGINAL-34: Performance Profiling | 1107–1153 | FAILED | performance |
| ORIGINAL-35: Verify Async Correctness | 1154–1179 | COMPLETED | review |
| ORIGINAL-36: Extreme Testing | 1180–1222 | COMPLETED | failures |
| ORIGINAL-37: Final Before vs After Benchmark | 1223–1287 | FAILED | performance |
| ORIGINAL-38: Final Requirement | 1288–1328 | BLOCKED BY ENVIRONMENT | live |
| COMBINED-01: Complete Previous Work First, Then Apply 429 Update | 1–58 | BLOCKED BY ENVIRONMENT | live |
| COMBINED-02: Mandatory Baseline Validation Before Modifying the Evil0ctal Code | 59–66 | COMPLETED | baseline |
| COMBINED-03: Required Baseline Validation | 67–87 | COMPLETED | baseline |
| COMBINED-04: Verify the TikTok Features We Actually Need | 88–136 | COMPLETED | protocol |
| COMBINED-05: Baseline Performance Measurements | 137–179 | BLOCKED BY ENVIRONMENT | live |
| COMBINED-06: Test a Known Follower Pagination Case | 180–198 | BLOCKED BY ENVIRONMENT | live |
| COMBINED-07: Environment Limitations | 199–208 | BLOCKED BY ENVIRONMENT | live |
| COMBINED-08: Preserve Baseline Evidence | 209–230 | COMPLETED | baseline |
| COMBINED-09: Do Not Begin Optimization Until Baseline Is Understood | 231–239 | COMPLETED | baseline |
| COMBINED-10: Additional 429 Behaviour | 240–243 | COMPLETED | rate |
| COMBINED-11: FindTik / External Worker 429 | 244–283 | COMPLETED | rate |
| COMBINED-12: Direct 429 | 284–335 | COMPLETED | rate |
| COMBINED-13: Direct Recovery Attempt | 336–358 | COMPLETED | rate |
| COMBINED-14: If Direct Gets 429 Again | 359–405 | COMPLETED | rate |
| COMBINED-15: Exact Final Flow | 406–437 | COMPLETED | rate |
| COMBINED-16: Important | 438–443 | COMPLETED | rate |
| COMBINED-17: Required Statistics | 444–462 | COMPLETED | metrics |
| COMBINED-18: Required Testing | 463–492 | COMPLETED | rate |
| COMBINED-19: Final Instruction | 493–525 | BLOCKED BY ENVIRONMENT | live |
| COMBINED-20: MAXIMUM PC-BOUND THROUGHPUT — NO ARTIFICIAL PERFORMANCE CAPS | 526–570 | FAILED | performance |
| COMBINED-21: No Arbitrary Internal Throughput Caps | 571–604 | COMPLETED | config |
| COMBINED-22: Workers Are the Main User-Controlled Concurrency Setting | 605–642 | COMPLETED | config |
| COMBINED-23: Connection Limits Must Scale From Worker Count | 643–679 | COMPLETED | config |
| COMBINED-24: Worker Count and Connection Count | 680–711 | COMPLETED | config |
| COMBINED-25: Maximum Request Throughput | 712–738 | FAILED | performance |
| COMBINED-26: Maximum Scraping and Parsing Throughput | 739–766 | FAILED | performance |
| COMBINED-27: Maximum Data Handling Throughput | 767–787 | FAILED | performance |
| COMBINED-28: Maximum Database / State Writing Throughput | 788–824 | COMPLETED | persistence |
| COMBINED-29: Maximum File-Writing Throughput | 825–846 | COMPLETED | persistence |
| COMBINED-30: Extremely Fast Logging and Printing | 847–869 | COMPLETED | metrics |
| COMBINED-31: Fast Statistics | 870–898 | COMPLETED | metrics |
| COMBINED-32: Async Everywhere It Actually Helps | 899–932 | COMPLETED | review |
| COMBINED-33: Avoid Worker Starvation | 933–949 | COMPLETED | rate |
| COMBINED-34: Queue Design | 950–970 | COMPLETED | review |
| COMBINED-35: Backend Throughput | 971–993 | COMPLETED | routing |
| COMBINED-36: Performance Should Be Limited by Reality, Not Arbitrary Constants | 994–1015 | FAILED | performance |
| COMBINED-37: Profiling Requirement | 1016–1039 | FAILED | performance |
| COMBINED-38: High-Worker Benchmark Requirement | 1040–1077 | FAILED | performance |
| COMBINED-39: Final Performance Goal | 1078–1113 | FAILED | performance |
| COMBINED-40: FINAL MANDATORY COMPLETION CHECKLIST | 1114–1309 | FAILED | final |
| COMBINED-41: Completion Rule | 1310–1332 | BLOCKED BY ENVIRONMENT | live |

The updated 429 rules in the combined specification supersede the original global-stop rule for Hybrid/Direct. Worker-only retains its established global stop. No page-size claim, live RPS result or upstream service throughput is inferred from fixtures. The required maximum reliable Direct page size and live baseline comparison remain blocked as described in `LIVE_VERIFICATION.md`.

Pending work is explicit in the JSON/CSV: finish the matched throughput/profile review, resolve regressions, confirm the latest native test gate, recheck both source documents and update this audit again. The final completion requirements remain open until their subordinate requirements are verified.
