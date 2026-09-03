"""Structural ratchets for the current PurrA package boundaries."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = ROOT_DIR / "src" / "purra"

FUNCTION_LINE_CAPS = {
    ("engine/orchestrator.py", "AgentCore.__init__"): 200,
    ("engine/orchestrator.py", "AgentCore._execute_run"): 450,
    ("runtime/orchestrator.py", "AgentRuntime.run"): 500,
}
MOVED_TOP_LEVEL_DEFINITIONS = {
    "runtime/orchestrator.py": {
        "_agent_model_call",
        "_ModelRoundAccumulator",
        "_PendingProviderAttempt",
        "_QueueEventSink",
        "_context_budget_contract_error",
        "_continuation_messages",
        "_declined_final_response",
        "_error_chain_types",
        "_exact_item_count_repair_guidance",
        "_failed_tool_final_response",
        "_future_step_retry_messages",
        "_is_deferred_action_only_response",
        "_is_recoverable_tool_input_error",
        "_is_retryable_stream_interruption",
        "_is_textual_tool_call",
        "_is_unstructured_tool_output",
        "_model_request_fingerprint",
        "_response_constraint_repair_guidance",
        "_root_error_type",
        "_should_hold_potential_deferred_response",
        "_stream_tool_batch",
        "_tool_authorization_recovery_scope",
        "_top_level_numbered_items",
        "_unauthorized_tool_retry_messages",
    },
    "runtime/tool_round.py": {
        "future_step_retry_messages",
        "is_recoverable_tool_input_error",
        "tool_authorization_recovery_scope",
        "unauthorized_tool_retry_messages",
    },
    "engine/orchestrator.py": {
        "AgentCoreRunOptions",
        "ContextStrategy",
        "ExecutionProfile",
        "PlanningCapability",
        "PlanningPhaseResult",
        "TaskOrchestrationCapability",
        "_AugmentedToolCatalog",
        "_BufferedEventSink",
        "_DynamicPlanningOrchestrator",
        "_assemble_messages",
        "_bind_event_to_run",
        "_compile_task_context_request",
        "_complete_admitted_task",
        "_context_demand_diagnostics",
        "_effective_registrations",
        "_host_planning_facts",
        "_merge_context_claims",
        "_planned_tool_names",
        "_planning_tool_guidance",
        "_runtime_output_event",
        "_validate_context_allocations",
        "_validate_plan_authority",
        "_validate_planning_constraints",
        "_validate_task_admission_coverage",
        "_validate_task_constraint_refinement",
        "_safe_model_only_plan",
    },
    "contracts/__init__.py": {
        "ApprovalDecision",
        "ApprovalStatus",
        "DelegationStatus",
        "MessageOrigin",
        "MessageRole",
        "ModelFinishReason",
        "PlanningKind",
        "ReasoningMode",
        "RunStatus",
        "RuntimeOutcome",
        "StepExecutor",
        "StepStatus",
        "StepType",
        "ToolBatchOutcome",
        "ToolChoiceMode",
        "ToolExecutionMode",
        "ToolRiskLevel",
        "ExecutionPlan",
        "ExecutionTransition",
        "TaskSpec",
        "TaskStep",
        "WorkPlan",
        "WorkStep",
    },
    "ports/__init__.py": {
        "RunBeginResult",
        "RunCommit",
        "RunRepository",
        "validate_run_commit_lifecycle",
    },
}

REQUIRED_CORE_MODULES = {
    "adapters/durable_memory.py",
    "adapters/memory.py",
    "agent_presets.py",
    "artifacts/ownership.py",
    "contracts/enums.py",
    "contracts/host.py",
    "contracts/plans.py",
    "context_strategies.py",
    "execution_profiles.py",
    "engine/canonical_sink.py",
    "engine/context_capability.py",
    "engine/context_phase.py",
    "engine/compaction_phase.py",
    "engine/delegation_assembly.py",
    "engine/durable_execution.py",
    "engine/dynamic_planning.py",
    "engine/options.py",
    "engine/planning_phase.py",
    "engine/planning_validation.py",
    "engine/tool_catalog.py",
    "engine/task_orchestration.py",
    "long_tasks/dispatcher.py",
    "observability/diagnostics.py",
    "observability/recovery.py",
    "ports/context.py",
    "ports/model.py",
    "ports/persistence.py",
    "ports/planning.py",
    "ports/projection.py",
    "ports/run_lifecycle.py",
    "ports/tools.py",
    "runtime/model_round.py",
    "output/response_validation.py",
    "runtime/response_finalization.py",
    "runtime/tool_authorization.py",
    "runtime/tool_recovery.py",
    "runtime/tool_round.py",
    "recovery/guidance.py",
    "run_recovery.py",
    "testing.py",
}


def _top_level_definitions(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.AsyncFunctionDef, ast.ClassDef, ast.FunctionDef))
    }


def _function_sizes(path: Path) -> dict[str, int]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    sizes: dict[str, int] = {}
    for node in tree.body:
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            sizes[node.name] = node.end_lineno - node.lineno + 1
        elif isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, (ast.AsyncFunctionDef, ast.FunctionDef)):
                    sizes[f"{node.name}.{child.name}"] = (
                        child.end_lineno - child.lineno + 1
                    )
    return sizes


def test_purra_uses_focused_packages():
    for legacy_name in ("contracts.py", "engine.py", "ports.py", "runtime.py"):
        assert not (CORE_DIR / legacy_name).exists(), legacy_name
    missing = sorted(
        relative
        for relative in REQUIRED_CORE_MODULES
        if not (CORE_DIR / relative).is_file()
    )
    assert not missing, "Missing PurrA modules: " + ", ".join(missing)
    assert not tuple((CORE_DIR / "work_items").glob("*.py"))
    assert not (CORE_DIR / "artifacts" / "scope.py").exists()


def test_durable_state_has_one_task_aggregate():
    all_core_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in CORE_DIR.rglob("*.py")
    )
    for removed_contract in (
        "purra.work_items",
        "WorkItem",
        "work_item_id",
        "complete_work_item",
        "ArtifactScope",
    ):
        assert removed_contract not in all_core_source


def test_observability_cannot_control_runtime():
    runtime_source = "\n".join(
        path.read_text(encoding="utf-8")
        for root in (CORE_DIR / "runtime", CORE_DIR / "engine")
        for path in root.rglob("*.py")
    )
    assert "purra.observability" not in runtime_source
    assert "purra.evaluation" not in runtime_source


def test_extracted_definitions_cannot_return_to_facades_or_orchestrators():
    violations: list[str] = []
    for relative, forbidden in MOVED_TOP_LEVEL_DEFINITIONS.items():
        observed = _top_level_definitions(CORE_DIR / relative)
        for name in sorted(observed & forbidden):
            violations.append(f"{relative} defines {name}")
    assert not violations, "PurrA responsibilities moved backwards:\n" + "\n".join(
        violations
    )


def test_tool_authorization_policy_stays_out_of_runtime_orchestrator():
    source = (CORE_DIR / "runtime" / "orchestrator.py").read_text(
        encoding="utf-8"
    )
    assert "resolve_tool_authorization(" in source
    for cause in (
        "RecoveryCause.FUTURE_TOOL_STEP",
        "RecoveryCause.UNAUTHORIZED_TOOL",
        "RecoveryCause.UNAUTHORIZED_TOOL_REPLAN",
    ):
        assert cause not in source


def test_tool_protocol_recovery_stays_out_of_runtime_orchestrator():
    source = (CORE_DIR / "runtime" / "orchestrator.py").read_text(
        encoding="utf-8"
    )
    assert "resolve_tool_protocol(" in source
    for cause in (
        "RecoveryCause.MALFORMED_TOOL_CALL_BATCH",
        "RecoveryCause.MISSING_REQUIRED_TOOL_CALL",
        "RecoveryCause.MISSING_REQUIRED_TOOL_CALL_REPLAN",
    ):
        assert cause not in source


def test_tool_execution_recovery_stays_out_of_runtime_orchestrator():
    source = (CORE_DIR / "runtime" / "orchestrator.py").read_text(
        encoding="utf-8"
    )
    assert "resolve_tool_recovery(" in source
    for cause in (
        "RecoveryCause.TOOL_INPUT_INVALID",
        "RecoveryCause.TOOL_EXECUTION_FAILED_REPLAN",
    ):
        assert cause not in source


def test_response_finalization_policy_stays_out_of_runtime_orchestrator():
    source = (CORE_DIR / "runtime" / "orchestrator.py").read_text(
        encoding="utf-8"
    )
    assert "resolve_response_recovery(" in source
    assert "resolve_response_constraint_recovery(" in source
    assert "public_presentation_messages(" in source
    for cause in (
        "RecoveryCause.UNSTRUCTURED_TOOL_PROTOCOL",
        "RecoveryCause.EMPTY_MODEL_RESPONSE",
        "RecoveryCause.RESPONSE_CONSTRAINT_DETERMINISTIC",
        "RecoveryCause.RESPONSE_CONSTRAINT_SEMANTIC",
    ):
        assert cause not in source


def test_response_validation_execution_has_one_coordinator():
    runtime = (CORE_DIR / "runtime" / "orchestrator.py").read_text(
        encoding="utf-8"
    )
    transaction = (CORE_DIR / "output" / "response_transaction.py").read_text(
        encoding="utf-8"
    )
    assert "ResponseValidationCoordinator(" in runtime
    assert "ResponseValidationCoordinator(" in transaction
    for source in (runtime, transaction):
        assert "validator.validate(" not in source
        assert "judge.judge(" not in source
        assert "OperationKind.VALIDATION" not in source
    assert "_start_validation_operation" not in runtime


def test_provider_recovery_stays_in_model_round_policy():
    runtime = (CORE_DIR / "runtime" / "orchestrator.py").read_text(
        encoding="utf-8"
    )
    model_round = (CORE_DIR / "runtime" / "model_round.py").read_text(
        encoding="utf-8"
    )
    assert "resolve_provider_failure(" in runtime
    assert "def resolve_provider_failure(" in model_round
    for policy_symbol in (
        "RecoveryCause.PROVIDER_REQUIRED_TOOL_CHOICE_UNSUPPORTED",
        "RecoveryCause.PROVIDER_STREAM_INTERRUPTED",
        '"provider_fallback_auto"',
        '"interrupted_retry"',
        "def _decide_recovery(",
    ):
        assert policy_symbol not in runtime


def test_public_facades_do_not_reexport_private_implementation_helpers():
    violations: list[str] = []
    for relative in ("engine/__init__.py", "runtime/__init__.py"):
        tree = ast.parse((CORE_DIR / relative).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            for alias in node.names:
                exported_name = alias.asname or alias.name.rsplit(".", 1)[-1]
                if exported_name.startswith("_"):
                    violations.append(f"{relative}: {exported_name}")
    assert not violations, "Private facade compatibility exports:\n" + "\n".join(
        violations
    )


def test_orchestrator_functions_stay_below_readability_caps():
    violations: list[str] = []
    all_sizes: dict[str, dict[str, int]] = {}
    for relative, qualified_name in FUNCTION_LINE_CAPS:
        sizes = all_sizes.setdefault(
            relative,
            _function_sizes(CORE_DIR / relative),
        )
        observed = sizes[qualified_name]
        cap = FUNCTION_LINE_CAPS[(relative, qualified_name)]
        if observed > cap:
            violations.append(
                f"{relative}:{qualified_name}: {observed} lines, cap {cap}"
            )
    for relative in {item[0] for item in FUNCTION_LINE_CAPS}:
        for qualified_name, observed in all_sizes[relative].items():
            if (
                qualified_name.split(".")[-1].startswith("_")
                and qualified_name not in {
                    "AgentCore.__init__",
                    "AgentCore._execute_run",
                }
                and observed > 300
            ):
                violations.append(
                    f"{relative}:{qualified_name}: {observed} lines, cap 300"
                )
    assert not violations, "PurrA functions grew:\n" + "\n".join(violations)


def test_internal_modules_have_no_import_cycles():
    module_paths: dict[str, Path] = {}
    for path in CORE_DIR.rglob("*.py"):
        parts = list(path.relative_to(CORE_DIR).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
        module = "purra" + (f".{'.'.join(parts)}" if parts else "")
        module_paths[module] = path

    edges = {module: set() for module in module_paths}
    for module, path in module_paths.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
            elif isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
        for target in imported:
            if not target.startswith("purra"):
                continue
            while target not in module_paths and "." in target:
                target = target.rsplit(".", 1)[0]
            if target in module_paths and target != module:
                edges[module].add(target)

    visited: set[str] = set()
    active: set[str] = set()

    def visit(module: str, chain: tuple[str, ...]) -> None:
        if module in active:
            start = chain.index(module)
            raise AssertionError(" -> ".join((*chain[start:], module)))
        if module in visited:
            return
        active.add(module)
        for target in sorted(edges[module]):
            visit(target, (*chain, module))
        active.remove(module)
        visited.add(module)

    for module in sorted(module_paths):
        visit(module, ())


def test_planning_boundary_cannot_recover_hidden_or_product_context():
    planner = (CORE_DIR / "planner.py").read_text(encoding="utf-8")
    orchestrator = (CORE_DIR / "engine" / "orchestrator.py").read_text(
        encoding="utf-8"
    )
    all_core_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in CORE_DIR.rglob("*.py")
    )

    assert "hostPlanningFacts" not in all_core_source
    assert "host_planning_facts" not in all_core_source
    for product_symbol in (
        "artifactContinuity",
        "artifactId",
        "workItemId",
        "replay_finalization",
    ):
        assert product_symbol not in planner
    assert "AgentPlanner(" not in orchestrator
    assert "request.planning_mode is PlanningMode.PLANNED" in orchestrator


def test_work_planning_and_runtime_authority_remain_separate():
    contracts = (CORE_DIR / "contracts" / "plans.py").read_text(
        encoding="utf-8"
    )
    planning_ports = (CORE_DIR / "ports" / "planning.py").read_text(
        encoding="utf-8"
    )
    runtime_ports = (CORE_DIR / "ports" / "tools.py").read_text(
        encoding="utf-8"
    )
    all_core_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in CORE_DIR.rglob("*.py")
    )

    assert "class WorkPlan" in contracts
    assert "class ExecutionPlan" in contracts
    assert "class ExecutionTransition" in contracts
    assert "class WorkPlanner" in planning_ports
    assert "current_execution_transition" in runtime_ports
    assert "TaskPlan" not in all_core_source
    assert "TaskPlanner" not in all_core_source
    assert "StepExecutor.AGENT" not in all_core_source
    assert "AgentAssignmentCoverage" not in all_core_source
    assert "agent_role_guidance" not in all_core_source
    assert "DelegatedAgentDefinition" not in all_core_source
    assert "build_general_delegated_agents" not in all_core_source
    assert "PresetDelegatedAgentExecutor" not in all_core_source


def test_recovery_persists_complete_execution_authority():
    lifecycle = (CORE_DIR / "ports" / "run_lifecycle.py").read_text(
        encoding="utf-8"
    )
    recovery = (CORE_DIR / "run_recovery.py").read_text(encoding="utf-8")
    continuation = (CORE_DIR / "engine" / "options.py").read_text(
        encoding="utf-8"
    )
    delegation = (CORE_DIR / "delegation" / "coordinator.py").read_text(
        encoding="utf-8"
    )
    contracts = (CORE_DIR / "contracts" / "__init__.py").read_text(
        encoding="utf-8"
    )
    persistence = (CORE_DIR / "ports" / "persistence.py").read_text(
        encoding="utf-8"
    )

    assert "replace_plan: ExecutionPlan" in lifecycle
    for bypass in (
        "replace_steps",
        "async def create(",
        "async def update_step(",
        "async def transition(",
    ):
        assert bypass not in lifecycle
    assert "class RunRecoverySnapshot" in recovery
    assert "execution_plan: ExecutionPlan" in recovery
    assert "source: RunRecoverySnapshot" in continuation
    assert "source_root_run_id" not in continuation
    assert "class DelegationCoordinator" in delegation
    for removed_child_run_contract in (
        "RunLineage",
        "child_run_id",
        "attach_child_run",
        "LiveDelegationCoordinator",
        "AgentDelegationCoordinator",
    ):
        assert removed_child_run_contract not in (
            delegation + contracts + persistence
        )
    assert "batch_id: str" in contracts
    assert "run_id: RunId" in contracts
    assert "class DelegatedAgentExecutor" in delegation
