"""Run the supported regression suite from any working directory."""
import os
import sys
import unittest
from pathlib import Path
sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0,str(ROOT/'tests'))
sys.path.insert(0,str(ROOT))
TESTS = [
 'test_hybrid_backend',
 'test_hybrid_review.ReviewTests', 'test_hybrid_review.WireReuseTests',
 'test_report_avatar_fix',
 'test_speed_update',
 'test_optimization.MetricsTests','test_optimization.RuntimeTests','test_optimization.RetryCrashTests',
 'test_optimization.ResumeSelectionTests.test_resume_yes_only_retries_selected_statuses',
 'test_optimization.ResumeSelectionTests.test_resume_no_keeps_failed_results_untouched',
 'test_optimization.ResumeSelectionTests.test_bootstrap_429_exits_as_rate_limit_and_preserves_cursor',
 'test_configured_scanner','test_async_scanner','test_proxy_wire',
 'test_persistent_scanner.DurableTests','test_persistent_scanner.ProcessTests',
 'test_prior_features.PriorTests',
 'test_prior_features.CountBootstrapTests.test_seven_count_cases_reach_main_scan',
 'test_latest_requirements.BootstrapFlowTests.test_real_kyriakakii_398_main_scan_and_resume',
 'test_latest_requirements.BootstrapFlowTests.test_empty_followers_restricted_following_clean_completion',
 'test_latest_requirements.BootstrapFlowTests.test_private_not_found_http_error_progress_all_count',
 'test_latest_requirements.BootstrapAcceptanceTests',
 'test_latest_requirements.RetryTests.test_profile_404_recovers_or_exactly_two_requests',
 'test_latest_requirements.RetryTests.test_list_404_recover_both_lists_and_saved_cursor_on_failure',
 'test_latest_requirements.RetryTests.test_404_after_hundreds_of_committed_pages',
 'test_latest_requirements.RestrictedStateTests',
 'test_release_checks.FinalChecks',
 'test_release_checks.FinalSetupTests.test_validation_is_last_step_pool_excludes_bad_proxy',
 'test_release_checks.FinalSetupTests.test_404_twice_main_no_list_requests',
 'test_release_checks.FinalSetupTests.test_profile_404_recovers_main_continues',
 'test_release_checks.ExtraRecoveryChecks','test_release_checks.SignalChecks',
 'test_run_folders.FolderTests',
 'test_avatar_update.AvatarTests', 'test_avatar_update.ReviewFixTests',
 'test_avatar_update.AvatarIntegrationTests.test_main_caches_public_private_and_skipped_avatars',
 'test_avatar_update.AvatarIntegrationTests.test_resume_backfills_old_records_and_reuses_saved_cache',
]
if os.name == 'nt':
    TESTS = [name for name in TESTS if name not in ('test_persistent_scanner.ProcessTests', 'test_release_checks.SignalChecks')]

if __name__ == '__main__':
    suite=unittest.defaultTestLoader.loadTestsFromNames(TESTS)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
