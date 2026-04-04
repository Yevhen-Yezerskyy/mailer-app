# FILE: engine/core_billing/billing.py
# DATE: 2026-04-04
# PURPOSE: Temporary billing helpers and audience task active recalculation.

from __future__ import annotations

from engine.common.db import get_connection


def get_workspace_daily_smtp_limit(workspace_id) -> int:
    # TODO: implement real daily SMTP limit for a workspace using SMTP hourly limits
    # and sending windows. For now billing uses a temporary default daily limit.
    return 500


def get_send_workspace(workspace_id) -> int:
    # TODO: implement real sent counter from the start of the rate period for a workspace.
    return 0


def get_send_task(task_id) -> int:
    # TODO: implement real sent counter from the start of the rate period for a task.
    return 0


def run_once() -> dict[str, int | str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                t.id::bigint,
                t.workspace_id::text,
                COALESCE(t.archived, false) AS archived,
                COALESCE(t.user_active, false) AS user_active,
                COALESCE(t.ready, false) AS ready,
                COALESCE(t.rate_limit, 0)::int AS rate_limit,
                COALESCE(w.access_type, '')::text AS access_type
            FROM public.aap_audience_audiencetask t
            LEFT JOIN public.accounts_workspaces w
              ON w.id = t.workspace_id
            ORDER BY t.id ASC
            """
        )
        task_rows = cur.fetchall() or []
        if not task_rows:
            return {
                "mode": "ok",
                "scanned_cnt": 0,
                "updated_cnt": 0,
                "active_true_cnt": 0,
                "active_false_cnt": 0,
            }

        cur.execute(
            """
            SELECT
                t.id::bigint AS task_id,
                COUNT(sl.aggr_contact_cb_id)::int AS sendable_cnt
            FROM public.aap_audience_audiencetask t
            LEFT JOIN public.sending_lists sl
              ON sl.task_id = t.id
             AND COALESCE(sl.removed, false) = false
             AND sl.rate IS NOT NULL
             AND sl.rate < COALESCE(t.rate_limit, 0)
            GROUP BY t.id
            """
        )
        sendable_by_task = {int(task_id): int(sendable_cnt or 0) for task_id, sendable_cnt in (cur.fetchall() or [])}

        cur.execute(
            """
            SELECT
                workspace_id::text,
                COALESCE(type::text, '') AS limit_type,
                sending_workspace_limit,
                sending_task_limit
            FROM public.accounts_workspace_limits
            """
        )
        limit_rows = cur.fetchall() or []

        limits_by_type: dict[str, tuple[int | None, int | None]] = {}
        limits_by_workspace: dict[str, tuple[int | None, int | None]] = {}
        for workspace_id, limit_type, workspace_limit, task_limit in limit_rows:
            limits_tuple = (
                int(workspace_limit) if workspace_limit is not None else None,
                int(task_limit) if task_limit is not None else None,
            )
            if workspace_id:
                limits_by_workspace[str(workspace_id)] = limits_tuple
            if limit_type:
                limits_by_type[str(limit_type)] = limits_tuple

        smtp_limit_cache: dict[str, int] = {}
        sent_workspace_cache: dict[str, int] = {}
        sent_task_cache: dict[int, int] = {}
        next_states: list[tuple[int, bool]] = []

        for task_id_raw, workspace_id, archived, user_active, ready, _rate_limit, access_type in task_rows:
            task_id = int(task_id_raw)
            workspace_id_str = str(workspace_id or "")
            access_type_str = str(access_type or "").strip()
            sendable_cnt = int(sendable_by_task.get(task_id, 0))

            if workspace_id_str not in smtp_limit_cache:
                smtp_limit_cache[workspace_id_str] = int(get_workspace_daily_smtp_limit(workspace_id_str) or 0)
            if workspace_id_str not in sent_workspace_cache:
                sent_workspace_cache[workspace_id_str] = int(get_send_workspace(workspace_id_str) or 0)
            if task_id not in sent_task_cache:
                sent_task_cache[task_id] = int(get_send_task(task_id) or 0)

            if access_type_str == "custom":
                limits = limits_by_workspace.get(workspace_id_str)
            else:
                limits = limits_by_type.get(access_type_str)

            is_active = False
            if (not bool(archived)) and bool(user_active) and bool(ready) and limits is not None:
                workspace_limit, task_limit = limits
                smtp_limit = int(smtp_limit_cache.get(workspace_id_str, 0))
                sent_workspace = int(sent_workspace_cache.get(workspace_id_str, 0))
                sent_task = int(sent_task_cache.get(task_id, 0))

                if (
                    workspace_limit is not None
                    and task_limit is not None
                    and (sendable_cnt + sent_workspace) < smtp_limit
                    and (sendable_cnt + sent_workspace) < int(workspace_limit)
                    and (sendable_cnt + sent_task) < int(task_limit)
                ):
                    is_active = True

            next_states.append((task_id, is_active))

        task_ids = [task_id for task_id, _is_active in next_states]
        active_values = [is_active for _task_id, is_active in next_states]

        cur.execute(
            """
            WITH data(task_id, active_value) AS (
                SELECT * FROM unnest(%s::bigint[], %s::boolean[])
            ),
            upd AS (
                UPDATE public.aap_audience_audiencetask t
                SET active = data.active_value,
                    updated_at = now()
                FROM data
                WHERE t.id = data.task_id
                  AND t.active IS DISTINCT FROM data.active_value
                RETURNING t.id, t.active
            )
            SELECT
                COUNT(*)::int AS updated_cnt
            FROM upd
            """,
            [task_ids, active_values],
        )
        updated_cnt = int((cur.fetchone() or [0])[0] or 0)
        conn.commit()

    active_true_cnt = sum(1 for _task_id, is_active in next_states if is_active)
    active_false_cnt = len(next_states) - active_true_cnt
    return {
        "mode": "ok",
        "scanned_cnt": int(len(next_states)),
        "updated_cnt": int(updated_cnt),
        "active_true_cnt": int(active_true_cnt),
        "active_false_cnt": int(active_false_cnt),
    }
