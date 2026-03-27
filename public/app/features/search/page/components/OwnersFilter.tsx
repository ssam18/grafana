import { css } from '@emotion/css';
import { useMemo } from 'react';

import { GrafanaTheme2, SelectableValue } from '@grafana/data';
import { t } from '@grafana/i18n';
import { Icon, MultiSelect, useStyles2 } from '@grafana/ui';
import { useSearchTeamsQuery } from 'app/api/clients/legacy';
import { teamOwnerRef } from 'app/features/browse-dashboards/utils/dashboards';

const ALL_TEAMS_VALUE = '__all-teams__';
const collator = new Intl.Collator();

interface OwnersFilterProps {
  ownerReference: string[];
  onChange: (ownerReference: string[]) => void;
}

export function OwnersFilter({ ownerReference, onChange }: OwnersFilterProps) {
  const styles = useStyles2(getStyles);
  const { data, isLoading } = useSearchTeamsQuery({ perpage: 100 });

  const teamOptions = useMemo<Array<SelectableValue<string>>>(() => {
    if (!data?.teams) {
      return [];
    }
    return data.teams
      .map((team) => ({
        label: team.name,
        value: teamOwnerRef(team),
        imgUrl: team.avatarUrl,
      }))
      .sort((a, b) => collator.compare(a.label ?? '', b.label ?? ''));
  }, [data?.teams]);

  const allTeamReferences = useMemo(() => {
    return teamOptions.flatMap((option) => (option.value ? [option.value] : []));
  }, [teamOptions]);

  const allTeamsLabel = t('browse-dashboards.filters.all-teams', 'All teams');

  const hasAllTeamsSelected =
    ownerReference.length > 0 &&
    allTeamReferences.length > 0 &&
    ownerReference.length === allTeamReferences.length &&
    allTeamReferences.every((reference) => ownerReference.includes(reference));

  const value = hasAllTeamsSelected
    ? [{ label: allTeamsLabel, value: ALL_TEAMS_VALUE }]
    : teamOptions.filter((option) => option.value && ownerReference.includes(option.value));

  const options = useMemo<Array<SelectableValue<string>>>(() => {
    if (teamOptions.length === 0) {
      return [];
    }

    return [{ label: allTeamsLabel, value: ALL_TEAMS_VALUE }, ...teamOptions];
  }, [teamOptions, allTeamsLabel]);

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
