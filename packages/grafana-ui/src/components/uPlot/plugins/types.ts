import { CSSProperties } from 'react';

import { LinkModel } from '@grafana/data';

import { AdHocFilterModel } from '../../VizTooltip/VizTooltipFooter';

import { TimeRange2 } from './TooltipPlugin2';

export interface LocalConsts {
  defaultStyles: Partial<CSSProperties>;
}

export interface LocalMutatableVars {
  yDrag: boolean;
  offsetX: number;
  offsetY: number;
  seriesIdxs: Array<number | null>;
  closestSeriesIdx: number | null;
  viaSync: boolean;
  dataLinks: LinkModel[];
  adHocFilters: AdHocFilterModel[];
  persistentLinks: LinkModel[][];
  pendingPinned: boolean;
  yZoomed: boolean;
  _someSeriesIdx: boolean;
  plotVisible: boolean;
  scrollbarWidth: number;
  winWid: number;
  winHgt: number;
  syncTooltip: boolean;
}
