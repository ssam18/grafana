import { getBackendSrv, setBackendSrv } from '@grafana/runtime';
import { installPluginMeta, uninstallPluginMeta } from '@grafana/runtime/internal';
import { setTestFlags } from '@grafana/test-utils/unstable';

import { installPlugin, uninstallPlugin } from './api';

jest.mock('@grafana/runtime/internal', () => ({
  ...jest.requireActual('@grafana/runtime/internal'),
  installPluginMeta: jest.fn(),
  uninstallPluginMeta: jest.fn(),
}));

const installPluginMetaMock = jest.mocked(installPluginMeta);
const uninstallPluginMetaMock = jest.mocked(uninstallPluginMeta);

describe('api', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    setBackendSrv({
      chunked: jest.fn(),
      delete: jest.fn(),
      fetch: jest.fn(),
      get: jest.fn(),
      patch: jest.fn(),
      post: jest.fn(),
      put: jest.fn(),
      request: jest.fn(),
      datasourceRequest: jest.fn(),
    });
    installPluginMetaMock.mockResolvedValue();
    uninstallPluginMetaMock.mockResolvedValue();
  });

  describe('when useMTPlugins flag is enabled', () => {
    beforeAll(() => {
      setTestFlags({ useMTPlugins: true });
    });

    afterAll(() => {
      setTestFlags({});
    });

    describe('installPlugin', () => {
      it('should call both legacy api and new api', async () => {
        await installPlugin('myorg-test-panel', '1.5.0');

        expect(installPluginMetaMock).toHaveBeenCalledTimes(1);
        expect(installPluginMetaMock).toHaveBeenCalledWith('myorg-test-panel', '1.5.0');
        expect(getBackendSrv().post).toHaveBeenCalledTimes(1);
        expect(getBackendSrv().post).toHaveBeenCalledWith(
          '/api/plugins/myorg-test-panel/install',
          { version: '1.5.0' },
          { showErrorAlert: false }
        );
      });

      it('should not call legacy api when installPluginMeta fails', async () => {
        installPluginMetaMock.mockRejectedValue(new Error('Network Error'));
        await expect(installPlugin('myorg-test-panel', '1.5.0')).rejects.toThrow('Network Error');
        expect(getBackendSrv().post).not.toHaveBeenCalled();
      });
    });

    describe('uninstallPlugin', () => {
      it('should call both legacy api and new api', async () => {
        await uninstallPlugin('myorg-test-panel');

        expect(uninstallPluginMetaMock).toHaveBeenCalledTimes(1);
        expect(uninstallPluginMetaMock).toHaveBeenCalledWith('myorg-test-panel');
        expect(getBackendSrv().post).toHaveBeenCalledTimes(1);
        expect(getBackendSrv().post).toHaveBeenCalledWith('/api/plugins/myorg-test-panel/uninstall');
      });

      it('should not call legacy api when uninstallPluginMeta fails', async () => {
        uninstallPluginMetaMock.mockRejectedValue(new Error('Network Error'));
        await expect(uninstallPlugin('myorg-test-panel')).rejects.toThrow('Network Error');
        expect(getBackendSrv().post).not.toHaveBeenCalled();
      });
    });
  });

  describe('when useMTPlugins flag is disabled', () => {
    beforeAll(() => {
      setTestFlags({ useMTPlugins: false });
    });

    afterAll(() => {
      setTestFlags({});
    });

    describe('installPlugin', () => {
      it('should only call legacy api', async () => {
        await installPlugin('myorg-test-panel', '1.5.0');

        expect(installPluginMetaMock).not.toHaveBeenCalled();
        expect(getBackendSrv().post).toHaveBeenCalledTimes(1);
        expect(getBackendSrv().post).toHaveBeenCalledWith(
          '/api/plugins/myorg-test-panel/install',
          { version: '1.5.0' },
          { showErrorAlert: false }
        );
      });
    });

    describe('uninstallPlugin', () => {
      it('should only call legacy api', async () => {
        await uninstallPlugin('myorg-test-panel');

        expect(uninstallPluginMetaMock).not.toHaveBeenCalled();
        expect(getBackendSrv().post).toHaveBeenCalledTimes(1);
        expect(getBackendSrv().post).toHaveBeenCalledWith('/api/plugins/myorg-test-panel/uninstall');
      });
    });
  });
});
