/** @vitest-environment jsdom */

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ProjectSessionTabs } from "@/app/components/projects/ProjectSessionTabs";
import type { ManagedSessionSummary, SessionBehavior } from "@/app/types/api";

vi.mock("@/app/hooks/useSessionTitle", () => ({
  useSessionTitle: () => (summary: { title: string | null }) => summary.title ?? "Untitled",
}));
vi.mock("@/app/hooks/useMediaQuery", () => ({
  useIsMobile: () => false,
}));
vi.mock("@/app/providers/ProjectActionsProvider", () => ({
  useProjectActions: () => ({ newChat: vi.fn(), assign: vi.fn() }),
}));
vi.mock("@/app/providers/SessionActionsProvider", () => ({
  useSessionActions: () => ({ remove: vi.fn() }),
}));

function session(
  sessionId: string,
  title: string,
  behavior: SessionBehavior,
): ManagedSessionSummary {
  return {
    active: false,
    active_run: null,
    lineage: null,
    summary: {
      backend: "openai-responses",
      behavior,
      created_at: "2026-08-28T12:00:00Z",
      cwd: "/workspace",
      forked_from: null,
      last_user_prompt: null,
      model: "gpt-5.6-sol",
      project_id: "project",
      sandboxed: false,
      session_id: sessionId,
      ssh_host: null,
      title,
      updated_at: "2026-08-28T12:00:00Z",
      visible_message_count: 1,
    },
    workspace_diff: null,
  };
}

afterEach(cleanup);

describe("project session tab behavior identity", () => {
  it("keeps flexible tabs readable and every behavior identifiable", () => {
    const sessions = [
      session("orchestrator", "Plan the managed deployment rollout", "orchestrator"),
      session("direct", "Implement connection status feedback", "direct"),
      session("hybrid", "Coordinate release readiness review", "direct-with-orchestrator"),
    ];
    render(
      <MemoryRouter>
        <ProjectSessionTabs
          projectId="project"
          sessions={sessions}
          activeSessionId="direct"
          summary={sessions[1].summary}
        />
      </MemoryRouter>,
    );

    const expected = [
      ["Plan the managed deployment rollout", "NAC orchestrator", "flow"],
      ["Implement connection status feedback", "Direct coding agent", "plane"],
      ["Coordinate release readiness review", "Direct + NAC orchestration", "combine"],
    ] as const;

    for (const [title, behavior, icon] of expected) {
      const tab = screen.getByRole("button", { name: `${title}, ${behavior}` });
      expect(tab.getAttribute("title")).toBe(title);
      expect(tab.querySelector(`[data-session-behavior-icon="${icon}"]`)).toBeTruthy();
      expect(tab.querySelector("[data-session-tab-badge]")).toBeNull();
      expect(tab.closest(".chat-session-tab")?.className).toContain("w-full");
      const slot = tab.closest(".chat-session-tab")?.parentElement;
      expect(slot?.className).toContain("flex-[1_0_224px]");
      expect(slot?.className).toContain("min-w-[224px]");
      expect(slot?.className).toContain("max-w-[272px]");
      expect(screen.getByRole("button", { name: `Close ${title}` })).toBeTruthy();
    }

    const active = screen.getByRole("button", {
      name: "Implement connection status feedback, Direct coding agent",
    });
    expect(active.getAttribute("aria-current")).toBe("page");
    expect(active.className).toContain("group-hover:pr-8");
    expect(active.className).toContain("group-has-[:focus-visible]:pr-8");

    expect(screen.queryByText("Orchestrator")).toBeNull();
    expect(screen.queryByText("Direct")).toBeNull();
    expect(screen.queryByText("Direct + NAC")).toBeNull();

    const orchestrator = screen.getByRole("button", {
      name: "Plan the managed deployment rollout, NAC orchestrator",
    });
    const orchestratorIcon = orchestrator.querySelector('[data-session-behavior-icon="flow"]');
    fireEvent.mouseEnter(orchestratorIcon!);
    expect(screen.getByText("NAC orchestrator").closest(".tooltip-box")).toBeTruthy();
    fireEvent.mouseLeave(orchestratorIcon!);

    fireEvent.focus(active);
    expect(screen.getByText("Direct coding agent").closest(".tooltip-box")).toBeTruthy();
  });
});
