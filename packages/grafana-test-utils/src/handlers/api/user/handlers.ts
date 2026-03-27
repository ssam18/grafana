import { HttpResponse, http } from 'msw';

import { wellFormedTree } from '../../../fixtures/folders';
import { mockStarredDashboardsMap } from '../../../fixtures/starred';
const [_, { dashbdD }] = wellFormedTree();

type SignedInUserTeam = {
  id?: number;
  uid: string;
  name: string;
  avatarUrl?: string;
};

const getStarsHandler = () =>
  http.get('/api/user/stars', async () => {
    return HttpResponse.json(Array.from(mockStarredDashboardsMap.keys()));
  });

const deleteDashboardStarHandler = () =>
  http.delete<{ uid: string }>('/api/user/stars/dashboard/uid/:uid', async ({ params }) => {
    const { uid } = params;
    mockStarredDashboardsMap.delete(uid);
    return HttpResponse.json({ message: 'Dashboard unstarred' });
  });

const addDashboardStarHandler = () =>
  http.post<{ uid: string }>('/api/user/stars/dashboard/uid/:uid', async ({ params }) => {
    const { uid } = params;
    mockStarredDashboardsMap.set(uid, true);
    return HttpResponse.json({ message: 'Dashboard starred!' });
  });

const getPreferencesHandler = () =>
  http.get('/api/user/preferences', async () => {
    return HttpResponse.json({
      homeDashboardUID: dashbdD.item.uid,
      theme: 'light',
      timezone: 'browser',
      weekStart: 'monday',
      queryHistory: {
        homeTab: '',
      },
      language: '',
    });
  });

const updatePreferencesHandler = () =>
  http.put('/api/user/preferences', async () => {
    return HttpResponse.json({ message: 'Preferences updated' });
  });

export const getSignedInUserTeamsHandler = (teams: SignedInUserTeam[]) =>
  http.get('/api/user/teams', async () => {
    return HttpResponse.json(teams);
  });

const handlers = [
  getPreferencesHandler(),
  updatePreferencesHandler(),
  getStarsHandler(),
  deleteDashboardStarHandler(),
  addDashboardStarHandler(),
];

export default handlers;
