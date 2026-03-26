import { css } from '@emotion/css';
import { useMemo } from 'react';

import { GrafanaTheme2, SelectableValue } from '@grafana/data';
import { t } from '@grafana/i18n';
import { config } from '@grafana/runtime';
import { Icon, MultiSelect, useStyles2 } from '@grafana/ui';
import { useGetSignedInUserTeamListQuery } from 'app/api/clients/legacy';
import { contextSrv } from 'app/core/services/context_srv';
import { ActionRow } from 'app/features/search/page/components/ActionRow';
import { getGrafanaSearcher } from 'app/features/search/service/searcher';
import { useSearchStateManager } from 'app/features/search/state/SearchStateManager';
import { AccessControlAction } from 'app/types/accessControl';

import { teamOwnerRef } from '../utils/dashboards';

const ALL_TEAMS_VALUE = '__all-teams__';
const collator = new Intl.Collator();

export function BrowseFilters() {
  const [searchState, stateManager] = useSearchStateManager();
  const additionalFilters = useBrowseOwnerFilter(searchState.ownerReference ?? [], stateManager.onOwnerReferenceChange);

  return (
    <ActionRow
      showStarredFilter
      showLayout
      state={searchState}
      additionalFilters={additionalFilters}
      getTagOptions={stateManager.getTagOptions}
      getSortOptions={getGrafanaSearcher().getSortOptions}
      sortPlaceholder={getGrafanaSearcher().sortPlaceholder}
      onLayoutChange={stateManager.onLayoutChange}
      onStarredFilterChange={stateManager.onStarredFilterChange}
      onSortChange={stateManager.onSortChange}
      onTagFilterChange={stateManager.onTagFilterChange}
      onDatasourceChange={stateManager.onDatasourceChange}
      onPanelTypeChange={stateManager.onPanelTypeChange}
      onSetIncludePanels={stateManager.onSetIncludePanels}
      onCreatedByChange={stateManager.onCreatedByChange}
    />
  );
}

function useBrowseOwnerFilter(ownerReference: string[], onChange: (ownerReference: string[]) => void) {
  const styles = useStyles2(getStyles);
  const canReadTeams = contextSrv.hasPermission(AccessControlAction.ActionTeamsRead);
  const supportsOwnerFilter =
    canReadTeams &&
    config.featureToggles.teamFolders &&
    (config.featureToggles.unifiedStorageSearchUI || config.featureToggles.panelTitleSearch);
  const { data: teams = [], isLoading } = useGetSignedInUserTeamListQuery(undefined, { skip: !supportsOwnerFilter });

  const teamOptions = useMemo<Array<SelectableValue<string>>>(() => {
    return teams
      .map((team) => ({
        label: team.name,
        value: teamOwnerRef(team),
        imgUrl: team.avatarUrl,
      }))
      .sort((a, b) => collator.compare(a.label ?? '', b.label ?? ''));
  }, [teams]);

  const allTeamReferences = useMemo(() => {
    return teamOptions.flatMap((option) => (option.value ? [option.value] : []));
  }, [teamOptions]);

  const hasAllTeamsSelected =
    ownerReference.length > 0 &&
    allTeamReferences.length > 0 &&
    ownerReference.length === allTeamReferences.length &&
    allTeamReferences.every((reference) => ownerReference.includes(reference));

  const value = hasAllTeamsSelected
    ? [{ label: t('browse-dashboards.filters.all-teams', 'All teams'), value: ALL_TEAMS_VALUE }]
    : teamOptions.filter((option) => option.value && ownerReference.includes(option.value));

  const options = useMemo<Array<SelectableValue<string>>>(() => {
    if (teamOptions.length === 0) {
      return [];
    }

    return [{ label: t('browse-dashboards.filters.all-teams', 'All teams'), value: ALL_TEAMS_VALUE }, ...teamOptions];
  }, [teamOptions]);

  if (!supportsOwnerFilter || (!isLoading && teamOptions.length === 0)) {
    return null;
  }

  return (
    <div className={styles.ownerFilter}>
      <MultiSelect<string>
        aria-label={t('browse-dashboards.filters.owner-aria-label', 'Owner filter')}
        options={options}
        value={value}
        onChange={(selectedOptions) => {
          const values = (selectedOptions ?? []).flatMap((option) => (option.value ? [option.value] : []));
          onChange(
            values.includes(ALL_TEAMS_VALUE) ? allTeamReferences : values.filter((value) => value !== ALL_TEAMS_VALUE)
          );
        }}
        getOptionLabel={(option) => option.label}
        getOptionValue={(option) => option.value ?? ''}
        noOptionsMessage={t('browse-dashboards.filters.owner-no-options', 'No teams found')}
        loadingMessage={t('browse-dashboards.filters.owner-loading', 'Loading teams...')}
        placeholder={t('browse-dashboards.filters.owner-placeholder', 'Filter by owner')}
        isLoading={isLoading}
        prefix={<Icon name="filter" />}
      />
    </div>
  );
}

const getStyles = (_theme: GrafanaTheme2) => ({
  ownerFilter: css({
    minWidth: '180px',
    flexGrow: 1,
  }),
});
