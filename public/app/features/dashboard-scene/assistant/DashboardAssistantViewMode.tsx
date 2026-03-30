import { useEffect } from 'react';

import { useAssistant } from '@grafana/assistant';
import { config } from '@grafana/runtime';
import { type SceneObject } from '@grafana/scenes';
import { useMediaQueryMinWidth } from 'app/core/hooks/useMediaQueryMinWidth';

import { useAssistantPanelHints } from './PanelAssistantHint';

interface DashboardAssistantViewModeProps {
  dashboard: SceneObject;
  isEditing: boolean | undefined;
}

/**
 * Manages assistant-related behavior in dashboard view mode:
 * - Checks assistant availability and feature toggle
 * - Injects AI sparkle hint icons on panels with data
 *
 * The popover is now triggered exclusively via the sparkle button (AssistantPopoverContext),
 * not through the element selection system. This prevents panel body clicks,
 * table column resizes, and other interactions from opening the popover.
 */
export function useDashboardAssistantViewMode({ dashboard, isEditing }: DashboardAssistantViewModeProps) {
  const { isAvailable: isAssistantAvailable, openAssistant } = useAssistant();
  // Disable on small screens: the 380px floating card doesn't fit, and the
  // auto-focused input inside AssistantPromptCard triggers the mobile keyboard.
  const isLargeScreen = useMediaQueryMinWidth('md');
  const isEnabled =
    !!config.featureToggles.dashboardAssistantPopover &&
    config.bootData.user.isSignedIn &&
    isAssistantAvailable &&
    isLargeScreen;

  useAssistantPanelHints(dashboard, isEditing, isEnabled);

  return {
    isEnabled,
    openAssistant,
  };
}

/**
 * Registers a global click-outside listener that calls `onDismiss` when the user
 * clicks anywhere except the popover or the sparkle hint buttons.
 */
export function usePopoverDismissOnClickOutside(isActive: unknown, onDismiss: () => void) {
  useEffect(() => {
    if (!isActive) {
      return;
    }

    const handler = (evt: PointerEvent) => {
      if (!(evt.target instanceof Element)) {
        return;
      }
      // Don't dismiss when clicking inside the popover or the sparkle hint
      if (
        evt.target.closest('[data-testid="view-mode-panel-prompt-card"]') ||
        evt.target.closest('[data-testid="panel-assistant-hint"]')
      ) {
        return;
      }
      onDismiss();
    };

    document.addEventListener('pointerdown', handler);
    return () => document.removeEventListener('pointerdown', handler);
  }, [isActive, onDismiss]);
}
