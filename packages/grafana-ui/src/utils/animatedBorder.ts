import { css, keyframes } from '@emotion/css';

import { type GrafanaTheme2 } from '@grafana/data';

import { type BackgroundColor } from '../components/Layout/Box/Box';
import { BORDER_ANGLE_PROPERTY, BORDER_OPACITY_PROPERTY } from '../themes/GlobalStyles/animatedBorder';

/** @public */
export type AnimatedBorderVariant = 'loading' | 'ai';

/** @public */
export interface AnimatedBorderOptions {
  variant: AnimatedBorderVariant;
  /** Animation duration in ms. Defaults to 2000. */
  duration?: number;
  /** Background color token used for the solid fill layer. Defaults to 'primary'. Uses the same values as Box. */
  backgroundColor?: BackgroundColor;
}

// TODO move these colors to theme
const AI_COLOR_1 = 'rgb(168, 85, 247)';
const AI_COLOR_2 = 'rgb(249, 115, 22)';
const DEFAULT_DURATION = 2000;

const borderRotate = keyframes({
  '0%': { [BORDER_ANGLE_PROPERTY]: '0deg' },
  '100%': { [BORDER_ANGLE_PROPERTY]: '360deg' },
});

const borderPulse = keyframes({
  '0%, 100%': { [BORDER_OPACITY_PROPERTY]: '1' },
  '50%': { [BORDER_OPACITY_PROPERTY]: '0.4' },
});

function resolveBackgroundColor(color: BackgroundColor, theme: GrafanaTheme2): string {
  switch (color) {
    case 'error':
    case 'success':
    case 'info':
    case 'warning':
      return theme.colors[color].transparent;
    default:
      return theme.colors.background[color];
  }
}

function getConicGradient(variant: AnimatedBorderVariant, theme: GrafanaTheme2): string {
  if (variant === 'loading') {
    return `conic-gradient(from var(${BORDER_ANGLE_PROPERTY}), transparent 60%, ${theme.colors.primary.main} 80%, ${theme.colors.primary.shade} 100%, transparent 15%)`;
  }
  return `conic-gradient(from var(${BORDER_ANGLE_PROPERTY}), transparent 60%, ${AI_COLOR_1} 80%, ${AI_COLOR_2} 100%, transparent 15%)`;
}

function mixWithBorderOpacity(color: string): string {
  return `color-mix(in srgb, ${color} calc(var(${BORDER_OPACITY_PROPERTY}) * 100%), transparent)`;
}

function getReducedMotionGradient(variant: AnimatedBorderVariant, theme: GrafanaTheme2): string {
  if (variant === 'loading') {
    const color = mixWithBorderOpacity(theme.colors.primary.main);
    return `linear-gradient(${color}, ${color})`;
  }
  return `linear-gradient(135deg, ${mixWithBorderOpacity(AI_COLOR_1)}, ${mixWithBorderOpacity(AI_COLOR_2)}, ${mixWithBorderOpacity(AI_COLOR_1)})`;
}

/**
 * Returns an Emotion className that applies an animated gradient border to an element.
 * Uses layered background-image with background-clip to render the gradient in the border area.
 * Conditionally apply via `cx()`.
 *
 * @public
 */
export function getAnimatedBorderStyles(theme: GrafanaTheme2, options: AnimatedBorderOptions): string {
  const { variant, duration = DEFAULT_DURATION, backgroundColor = 'primary' } = options;
  const bg = resolveBackgroundColor(backgroundColor, theme);

  return css({
    outline: 'none',
    backgroundOrigin: 'border-box',
    // TODO this can be replaced with `border-area` once it becomes more widely supported
    backgroundClip: 'padding-box, border-box',

    [theme.transitions.handleMotion('no-preference')]: {
      backgroundImage: `linear-gradient(${bg}, ${bg}), ${getConicGradient(variant, theme)}`,
      animation: `${borderRotate} ${duration}ms linear infinite`,
    },
    [theme.transitions.handleMotion('reduce')]: {
      backgroundImage: `linear-gradient(${bg}, ${bg}), ${getReducedMotionGradient(variant, theme)}`,
      animation: `${borderPulse} ${duration}ms ease-in-out infinite`,
    },
  });
}
