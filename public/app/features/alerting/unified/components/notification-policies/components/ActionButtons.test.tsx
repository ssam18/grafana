import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render } from 'test/test-utils';

import { ROUTES_META_SYMBOL, Route } from '../../../../../../plugins/datasource/alertmanager/types';
import { useAlertmanagerAbilities } from '../../../hooks/useAbilities';
import { K8sAnnotations } from '../../../utils/k8s/constants';

import { ActionButtons } from './ActionButtons';

jest.mock('../../../hooks/useAbilities', () => ({
  ...jest.requireActual('../../../hooks/useAbilities'),
  useAlertmanagerAbilities: jest.fn(),
}));

jest.mock('../useExportRoutingTree', () => ({
  useExportRoutingTree: () => [null, jest.fn()],
}));

jest.mock('../useNotificationPolicyRoute', () => ({
  ...jest.requireActual('../useNotificationPolicyRoute'),
  useDeleteRoutingTree: () => [{ execute: jest.fn() }],
}));

const useAlertmanagerAbilitiesMock = jest.mocked(useAlertmanagerAbilities);

function grantAllAbilities() {
  useAlertmanagerAbilitiesMock.mockReturnValue([
    [true, true],
    [true, true],
    [true, true],
  ]);
}

function makeRoute(annotations?: Record<string, string>): Route {
  const route: Route = {
    name: 'test-route',
    receiver: 'grafana-default-email',
  };

  if (annotations) {
    route[ROUTES_META_SYMBOL] = {
      name: 'test-route',
      metadata: { annotations },
    };
  }

  return route;
}

describe('ActionButtons', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    grantAllAbilities();
  });

  it('should show manage permissions button when route has canAdmin annotation', () => {
    const route = makeRoute({
      [K8sAnnotations.AccessAdmin]: 'true',
    });

    render(<ActionButtons route={route} />);

    expect(screen.getByTestId('manage-permissions-action')).toBeInTheDocument();
  });

  it('should not show manage permissions button when route lacks canAdmin annotation', () => {
    const route = makeRoute();

    render(<ActionButtons route={route} />);

    expect(screen.queryByTestId('manage-permissions-action')).not.toBeInTheDocument();
  });

  it('should not show manage permissions button when canAdmin is false', () => {
    const route = makeRoute({
      [K8sAnnotations.AccessAdmin]: 'false',
    });

    render(<ActionButtons route={route} />);

    expect(screen.queryByTestId('manage-permissions-action')).not.toBeInTheDocument();
  });

  it('should open permissions drawer when manage permissions button is clicked', async () => {
    jest.spyOn(console, 'error').mockImplementation();

    const user = userEvent.setup();

    const route = makeRoute({
      [K8sAnnotations.AccessAdmin]: 'true',
    });

    render(<ActionButtons route={route} />);

    await user.click(screen.getByTestId('manage-permissions-action'));

    expect(screen.getByRole('dialog', { name: /manage permissions/i })).toBeInTheDocument();
  });
});
