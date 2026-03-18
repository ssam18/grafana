import { waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render } from 'test/test-utils';
import { byRole } from 'testing-library-selector';

import { setupDataSources } from 'app/features/alerting/unified/testSetup/datasources';
import { AccessControlAction } from 'app/types/accessControl';

import { setupMswServer } from '../../mockApi';
import { grantUserPermissions } from '../../mocks';
import { AlertmanagerProvider } from '../../state/AlertmanagerContext';

import AlertmanagerConfig from './AlertmanagerConfig';
import {
  EXTERNAL_VANILLA_ALERTMANAGER_UID,
  PROVISIONED_MIMIR_ALERTMANAGER_UID,
  mockDataSources,
  setupVanillaAlertmanagerServer,
} from './mocks/server';

const renderConfiguration = (
  alertManagerSourceName: string,
  { onDismiss = jest.fn(), onSave = jest.fn() }
) =>
  render(
    <AlertmanagerProvider accessType="instance">
      <AlertmanagerConfig
        alertmanagerName={alertManagerSourceName}
        onDismiss={onDismiss}
        onSave={onSave}
      />
    </AlertmanagerProvider>
  );

const ui = {
  saveButton: byRole('button', { name: /Save/ }),
  cancelButton: byRole('button', { name: /Cancel/ }),
};

const waitForEditableConfig = async () => {
  await waitFor(() => expect(ui.saveButton.get()).toBeEnabled());
};

describe('Alerting Settings', () => {
  setupMswServer();

  beforeEach(() => {
    grantUserPermissions([AccessControlAction.AlertingNotificationsRead, AccessControlAction.AlertingInstanceRead]);
  });

  it('should be able to cancel', async () => {
    const onDismiss = jest.fn();
    renderConfiguration('grafana', { onDismiss });

    await userEvent.click(await ui.cancelButton.get());
    expect(onDismiss).toHaveBeenCalledTimes(1);
  });
});

describe('vanilla Alertmanager', () => {
  const server = setupMswServer();

  beforeEach(() => {
    setupVanillaAlertmanagerServer(server);
    setupDataSources(...Object.values(mockDataSources));
    grantUserPermissions([AccessControlAction.AlertingNotificationsRead, AccessControlAction.AlertingInstanceRead]);
  });

  afterAll(() => {
    jest.resetAllMocks();
  });

  it('should be read-only when using vanilla Prometheus Alertmanager', async () => {
    renderConfiguration(EXTERNAL_VANILLA_ALERTMANAGER_UID, {});

    expect(ui.cancelButton.get()).toBeInTheDocument();
    expect(ui.saveButton.query()).not.toBeInTheDocument();
  });

  it('should not be read-only when Mimir Alertmanager', async () => {
    renderConfiguration(PROVISIONED_MIMIR_ALERTMANAGER_UID, {});

    expect(ui.cancelButton.get()).toBeInTheDocument();
    expect(ui.saveButton.get()).toBeInTheDocument();
    await waitForEditableConfig();
  });
});
