import { css, cx } from '@emotion/css';
import { type Meta, type StoryFn } from '@storybook/react';

import { type GrafanaTheme2 } from '@grafana/data';

import { useStyles2 } from '../themes/ThemeContext';
import { DashboardStoryCanvas } from '../utils/storybook/DashboardStoryCanvas';

import { type AnimatedBorderOptions, getAnimatedBorderStyles } from './animatedBorder';

const meta: Meta = {
  title: 'Utilities/AnimatedBorder',
  parameters: {
    controls: {},
  },
};

export default meta;

interface StoryArgs extends AnimatedBorderOptions {
  active: boolean;
}

const getStyles = (theme: GrafanaTheme2) => ({
  container: css({
    backgroundColor: theme.colors.background.primary,
    width: 400,
    height: 200,
    border: `1px solid ${theme.components.panel.borderColor}`,
    borderRadius: theme.shape.radius.default,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    color: theme.colors.text.secondary,
    overflow: 'hidden',
  }),
});

export const Basic: StoryFn<StoryArgs> = ({ active, variant, duration }) => {
  const styles = useStyles2(getStyles);
  const animatedBorder = useStyles2(getAnimatedBorderStyles, { variant, duration });

  return (
    <DashboardStoryCanvas>
      <div className={cx(styles.container, { [animatedBorder]: active })}>Animated border</div>
    </DashboardStoryCanvas>
  );
};

Basic.args = {
  active: true,
  variant: 'loading',
  duration: 2000,
};

Basic.argTypes = {
  active: { control: 'boolean' },
  variant: { control: 'select', options: ['loading', 'ai'] },
};
