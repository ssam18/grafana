import { css, cx } from '@emotion/css';
import * as React from 'react';
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import uPlot from 'uplot';

import { GrafanaTheme2, LinkModel } from '@grafana/data';
import { DashboardCursorSync } from '@grafana/schema';

import { usePlotConfigHook } from '../../../../../../public/app/plugins/panel/timeseries/hooks/usePlotConfigHook';
import { AdHocFilterModel } from '../../../internal';
import { useStyles2 } from '../../../themes/ThemeContext';
import { OnSelectRangeCallback, RangeSelection1D, RangeSelection2D } from '../../PanelChrome';
import { getPortalContainer } from '../../Portal/Portal';
import { UPlotConfigBuilder } from '../config/UPlotConfigBuilder';

import { CloseButton } from './CloseButton';
import { initConstVars, initMutatableVars } from './TooltipUtils';

export const DEFAULT_TOOLTIP_WIDTH = undefined;
export const TOOLTIP_OFFSET = 10;

// todo: barchart? histogram?
export const enum TooltipHoverMode {
  // Single mode in TimeSeries, Candlestick, Trend, StateTimeline, Heatmap?
  xOne,
  // All mode in TimeSeries, Candlestick, Trend, StateTimeline, Heatmap?
  xAll,
  // Single mode in XYChart, Heatmap?
  xyOne,
}

type GetDataLinksCallback = (seriesIdx: number, dataIdx: number) => LinkModel[];
type GetAdHocFiltersCallback = (seriesIdx: number, dataIdx: number) => AdHocFilterModel[];

interface TooltipPlugin2Props {
  config: UPlotConfigBuilder;
  hoverMode: TooltipHoverMode;

  syncMode?: DashboardCursorSync;
  syncScope?: string;

  // x only
  queryZoom?: (range: { from: number; to: number }) => void;
  // y-only, via shiftKey
  clientZoom?: boolean;

  onSelectRange?: OnSelectRangeCallback;
  getDataLinks?: GetDataLinksCallback;
  getAdHocFilters?: GetAdHocFiltersCallback;

  render: (
    u: uPlot,
    dataIdxs: Array<number | null>,
    seriesIdx: number | null,
    isPinned: boolean,
    dismiss: (u: uPlot) => void,
    // selected time range (for annotation triggering)
    timeRange: TimeRange2 | null,
    viaSync: boolean,
    dataLinks: LinkModel[],
    adHocFilters: AdHocFilterModel[]
  ) => React.ReactNode;

  maxWidth?: number;
}

interface TooltipContainerSize {
  observer: ResizeObserver;
  width: number;
  height: number;
}

export interface TimeRange2 {
  from: number;
  to: number;
}

// min px width that triggers zoom
const MIN_ZOOM_DIST = 5;

const maybeZoomAction = (e?: MouseEvent | null) => e != null && !e.ctrlKey && !e.metaKey;

const getDataLinksFallback: GetDataLinksCallback = () => [];
const getAdHocFiltersFallback: GetAdHocFiltersCallback = () => [];

const userAgentIsMobile = /Android|iPhone|iPad/i.test(navigator.userAgent);

/**
 * @alpha
 */
export const TooltipPlugin2 = ({
  config,
  hoverMode,
  render,
  clientZoom = false,
  queryZoom,
  onSelectRange,
  maxWidth,
  syncMode = DashboardCursorSync.Off,
  syncScope = 'global', // eventsScope
  getDataLinks = getDataLinksFallback,
  getAdHocFilters = getAdHocFiltersFallback,
}: TooltipPlugin2Props) => {
  const styles = useStyles2(getStyles, maxWidth);
  const domRef = useRef<HTMLDivElement>(null);
  const portalRoot = useRef<HTMLElement | null>(null);

  const [isHovering, setIsHovering] = useState(false);
  // @todo hook back up?
  const [pendingRender, setPendingRender] = useState(false);
  const [isPinned, setIsPinned] = useState(false);
  const [tooltipContent, setTooltipContent] = useState<React.ReactNode>(null);
  const [style, setStyle] = useState<Partial<React.CSSProperties>>({ transform: '', pointerEvents: 'none' });
  const [annotationRange, setAnnotationRange] = useState<TimeRange2 | null>(null);

  if (portalRoot.current == null) {
    portalRoot.current = getPortalContainer();
  }

  // Init uplot
  const plot = usePlotConfigHook(config, 'init', (u) => {
    // detect shiftKey and mutate drag mode from x-only to y-only
    if (clientZoom) {
      u.over.addEventListener(
        'mousedown',
        (e) => {
          if (!maybeZoomAction(e)) {
            return;
          }

          if (e.button === 0 && e.shiftKey) {
            yDrag = true;

            u.cursor.drag!.x = false;
            u.cursor.drag!.y = true;

            let onUp = (e: MouseEvent) => {
              u.cursor.drag!.x = true;
              u.cursor.drag!.y = false;
              document.removeEventListener('mouseup', onUp, true);
            };

            document.addEventListener('mouseup', onUp, true);
          }
        },
        true
      );
    }

    // add zoom-in cursor during drag-to-zoom interaction
    if (queryZoom != null || clientZoom) {
      u.over.addEventListener(
        'mousedown',
        (e) => {
          if (!maybeZoomAction(e)) {
            return;
          }

          if (e.button === 0) {
            u.over.classList.add('zoom-drag');

            let onUp = () => {
              u.over.classList.remove('zoom-drag');
              document.removeEventListener('mouseup', onUp, true);
            };

            document.addEventListener('mouseup', onUp, true);
          }
        },
        true
      );
    }

    // this handles pinning, 0-width range selection, and one-click
    // @todo clean up event listener on uplot destruct
    u.over.addEventListener('click', (e) => {
      if (!u.cursor) {
        throw new Error('[TooltipPlugin2::onClick] cursor not defined!');
      }
      if (u.cursor.left === undefined) {
        throw new Error('[TooltipPlugin2::onClick] cursor left undefined!');
      }

      console.log('[TooltipPlugin2::onClick]', { e, over: u.over, ctrl: e.ctrlKey, meta: e.metaKey });
      if (e.target === u.over) {
        console.log('[TooltipPlugin2::onClick] - target is u.over');
        const isWipAnnotationModifierActive = e.ctrlKey || e.metaKey;
        if (isWipAnnotationModifierActive) {
          let xVal;

          const isXAxisHorizontal = u.scales.x.ori === 0;
          if (isXAxisHorizontal) {
            xVal = u.posToVal(u.cursor.left, 'x');
          } else {
            xVal = u.posToVal(u.select.top + u.select.height, 'x');
          }

          setAnnotationRange({
            from: xVal,
            to: xVal,
          });

          console.log('[TooltipPlugin2::onClick] - renderTooltip (anno)');
          renderTooltip(u, false);
        }
        // if tooltip visible, not pinned, and within proximity to a series/point
        else if (isHovering && !isPinned && closestSeriesIdx != null) {
          const seriesIndex = seriesIdxs[closestSeriesIdx];
          if (seriesIndex === null) {
            throw new Error('[TooltipPlugin2::onClick] - seriesIndex not defined!');
          }
          dataLinks = getLinksRef.current(closestSeriesIdx, seriesIndex);
          adHocFilters = getAdHocFiltersRef.current(closestSeriesIdx, seriesIndex);
          const oneClickLink = dataLinks.find((dataLink) => dataLink.oneClick === true);

          if (oneClickLink != null) {
            window.open(oneClickLink.href, oneClickLink.target ?? '_self');
          } else {
            setIsPinned(true);
            console.log('[TooltipPlugin2::onClick] - renderTooltip (hover)');
            renderTooltip(u, true);
          }
        }
      }
    });

    updateWinSize(u);

    console.log('init complete');
  });

  // Refs
  const sizeRef = useRef<TooltipContainerSize | undefined>(undefined);
  const renderRef = useRef(render);
  renderRef.current = render;

  const getLinksRef = useRef(getDataLinks);
  getLinksRef.current = getDataLinks;

  const getAdHocFiltersRef = useRef(getAdHocFilters);
  getAdHocFiltersRef.current = getAdHocFilters;
  let {
    yDrag,
    offsetX,
    offsetY,
    seriesIdxs,
    closestSeriesIdx,
    viaSync,
    dataLinks,
    adHocFilters,
    persistentLinks,
    pendingPinned,
    yZoomed,
    _someSeriesIdx,
    plotVisible,
    scrollbarWidth,
    winWid,
    winHgt,
    syncTooltip,
  } = initMutatableVars(syncMode);

  const { defaultStyles } = initConstVars(style);

  if (syncMode !== DashboardCursorSync.Off && config.scales[0].props.isTime) {
    config.setCursor({
      sync: {
        key: syncScope,
        scales: ['x', null],
      },
    });
  }

  function setTooltipState(u: uPlot, hover = false) {
    if (!u) {
      throw new Error('[TooltipPlugin2::setTooltipState] plot not initiated!');
    }

    console.log('[TooltipPlugin2::setTooltipState]', { hover, u });

    setPendingRender(false);
    let pointerEventsStyles: Partial<React.CSSProperties> | null = null;

    if (pendingPinned) {
      pointerEventsStyles = { pointerEvents: isPinned ? 'all' : 'none' };

      //@ts-expect-error access private method @todo narrow
      u.cursor['_lock'] = _isPinned;

      if (isPinned) {
        document.addEventListener('mousedown', downEventOutside, true);
        document.addEventListener('keydown', downEventOutside, true);
      } else {
        document.removeEventListener('mousedown', downEventOutside, true);
        document.removeEventListener('keydown', downEventOutside, true);
      }

      pendingPinned = false;
    }

    // @todo clean up
    // setStyle(pointerEventsStyles ?? defaultStyles);
    // setIsPinned(isPinned);
    if (hover !== isHovering) {
      console.log('[TooltipPlugin2::setTooltipState] setIsHovering');
      setIsHovering(hover);
    }

    const isWipAnnotationActive = annotationRange != null;
    const isTooltipActive = isWipAnnotationActive || hover;

    console.log('[TooltipPlugin2::setTooltipState] setTooltipContent', {
      hover,
      isWipAnnotationActive,
      isPinned,
      style: pointerEventsStyles ?? defaultStyles,
      isTooltipActive,
      annotationRange,
    });

    setTooltipContent(
      isTooltipActive
        ? renderRef.current(
            u,
            seriesIdxs,
            closestSeriesIdx,
            isPinned,
            dismiss,
            annotationRange,
            viaSync,
            isPinned ? dataLinks : closestSeriesIdx != null ? persistentLinks[closestSeriesIdx] : [],
            isPinned ? adHocFilters : []
          )
        : null
    );

    // TODO: set u.over.style.cursor = 'pointer' if we hovered a oneClick point
    // else revert to default...but only when the new pointer is different from prev
  }

  function dismiss(u: uPlot) {
    if (!u) {
      throw new Error('[TooltipPlugin2::dismiss] - plot not initiated!');
    }
    console.log('[TooltipPlugin2::dismiss]', { isHovering, isPinned });
    const prevIsPinned = isPinned;
    setIsPinned(false);
    setIsHovering(false);
    setAnnotationRange(null);
    u.setCursor({ left: -10, top: -10 });
    dataLinks = [];
    adHocFilters = [];

    console.log('[TooltipPlugin2::dismiss] renderTooltip');
    renderTooltip(u, prevIsPinned);
  }

  // set tooltip on annotation range state change
  useEffect(() => {
    if (!plot) {
      return;
    }

    setTooltipState(plot);
  }, [annotationRange, plot]);

  // set tooltip on hovering state change
  useEffect(() => {
    if (!plot) {
      return;
    }

    console.log('onHover', isHovering);
    setTooltipState(plot, isHovering);
  }, [isHovering, plot]);

  function renderTooltip(u: uPlot, setHover = true) {
    if (!u) {
      throw new Error('[TooltipPlugin2::renderTooltip] Plot is not defined!');
    }

    console.log('[TooltipPlugin2::renderTooltip]', { isHovering, u, setHover, annotationRange });

    // if(setHover !== isHovering){
    //   setTooltipState(u, setHover);
    // }

    // defer unrender for 100ms to reduce flickering in small gaps
    // if (!isHovering) {
    //   setTooltipState(u, true);
    //   // setTimeout(setTooltipState, 100);
    // } else {
    //   setTooltipState(u, true);
    //   // queueMicrotask(() => setTooltipState(true));
    // }

    if (!pendingRender) {
      setPendingRender(true);
    }

    setIsHovering(setHover);
  }

  function updateWinSize(u: uPlot) {
    isHovering && !isPinned && dismiss(u);

    winWid = window.innerWidth - scrollbarWidth;
    winHgt = window.innerHeight - scrollbarWidth;
  }

  function updatePlotVisible(plot: uPlot | null) {
    if (!plot) {
      throw new Error('[updatePlotVisible] Plot not initiated!');
    }
    plotVisible = plot.rect.bottom <= winHgt && plot.rect.top >= 0 && plot.rect.left >= 0 && plot.rect.right <= winWid;
  }

  function isUserHovering() {
    return viaSync
      ? plotVisible && _someSeriesIdx && syncTooltip
      : closestSeriesIdx != null || (hoverMode === TooltipHoverMode.xAll && _someSeriesIdx);
  }

  // in some ways this is similar to ClickOutsideWrapper.tsx
  function downEventOutside(e: Event) {
    if (!plot) {
      throw new Error('[TooltipPlugin2::downEventOutside] plot is undefined!');
    }
    if (e instanceof KeyboardEvent) {
      if (e.key === 'Escape') {
        e.preventDefault();
        e.stopPropagation();
        dismiss(plot);
      }
      return;
    }

    // this tooltip is Portaled, but actions inside it create forms in Modals
    const isModalOrPortaled = '[role="dialog"], #grafana-portal-container';

    // eslint-disable-next-line @typescript-eslint/consistent-type-assertions
    if ((e.target as HTMLElement).closest(isModalOrPortaled) == null) {
      dismiss(plot);
    }
  }

  // fires on data value hovers/unhovers
  usePlotConfigHook(config, 'setLegend', (u) => {
    if (!u.cursor.idxs) {
      throw new Error('cursor must be defined!');
    }

    seriesIdxs = u.cursor.idxs.slice();
    _someSeriesIdx = seriesIdxs.some((v, i) => i > 0 && v != null);
    if (persistentLinks.length === 0) {
      persistentLinks = seriesIdxs.map((v, seriesIdx) => {
        if (seriesIdx > 0) {
          const links = getDataLinks(seriesIdx, seriesIdxs[seriesIdx]!);
          const oneClickLink = links.find((dataLink) => dataLink.oneClick === true);

          if (oneClickLink) {
            return [oneClickLink];
          }
        }

        return [];
      });
    }

    viaSync = u.cursor.event == null;
    let prevIsHovering = isHovering;
    const currentIsHovering = isUserHovering();

    if (currentIsHovering || currentIsHovering !== prevIsHovering) {
      console.log('[TooltipPlugin2::setLegend] renderTooltip hover - true');
      renderTooltip(u, true);
    } else {
      console.log('[TooltipPlugin2::setLegend] renderTooltip hover - false');
      renderTooltip(u, false);
    }
  });
  usePlotConfigHook(config, 'setSelect', (u) => {
    const isXAxisHorizontal = u.scales.x.ori === 0;

    if (!viaSync && (clientZoom || queryZoom != null)) {
      if (maybeZoomAction(u.cursor!.event)) {
        if (onSelectRange != null) {
          let selections: RangeSelection2D[] = [];

          const yDrag = Boolean(u.cursor!.drag!.y);
          const xDrag = Boolean(u.cursor!.drag!.x);

          let xSel = null;
          let ySels: RangeSelection1D[] = [];

          // get x selection
          if (xDrag) {
            xSel = {
              from: isXAxisHorizontal
                ? u.posToVal(u.select.left!, 'x')
                : u.posToVal(u.select.top + u.select.height, 'x'),
              to: isXAxisHorizontal ? u.posToVal(u.select.left! + u.select.width, 'x') : u.posToVal(u.select.top, 'x'),
            };
          }

          // get y selections
          if (yDrag) {
            config.scales.forEach((scale) => {
              const key = scale.props.scaleKey;

              if (key !== 'x') {
                let ySel = {
                  from: isXAxisHorizontal
                    ? u.posToVal(u.select.top + u.select.height, key)
                    : u.posToVal(u.select.left + u.select.width, key),
                  to: isXAxisHorizontal ? u.posToVal(u.select.top, key) : u.posToVal(u.select.left, key),
                };

                ySels.push(ySel);
              }
            });
          }

          if (xDrag) {
            if (yDrag) {
              // x + y
              selections = ySels.map((ySel) => ({ x: xSel!, y: ySel }));
            } else {
              // x only
              selections = [{ x: xSel! }];
            }
          } else {
            if (yDrag) {
              // y only
              selections = ySels.map((ySel) => ({ y: ySel }));
            }
          }

          onSelectRange(selections);
        } else if (clientZoom && yDrag) {
          if (u.select.height >= MIN_ZOOM_DIST) {
            for (let key in u.scales!) {
              if (key !== 'x') {
                const maxY = isXAxisHorizontal
                  ? u.posToVal(u.select.top, key)
                  : u.posToVal(u.select.left + u.select.width, key);
                const minY = isXAxisHorizontal
                  ? u.posToVal(u.select.top + u.select.height, key)
                  : u.posToVal(u.select.left, key);

                u.setScale(key, { min: minY, max: maxY });
              }
            }

            yZoomed = true;
          }

          yDrag = false;
        } else if (queryZoom != null) {
          if (u.select.width >= MIN_ZOOM_DIST) {
            const minX = isXAxisHorizontal
              ? u.posToVal(u.select.left, 'x')
              : u.posToVal(u.select.top + u.select.height, 'x');
            const maxX = isXAxisHorizontal
              ? u.posToVal(u.select.left + u.select.width, 'x')
              : u.posToVal(u.select.top, 'x');

            queryZoom({ from: minX, to: maxX });

            yZoomed = false;
          }
        }
      } else {
        console.log('[TooltipPlugin2::setSelect] setAnnotationRange');
        setAnnotationRange({
          from: isXAxisHorizontal ? u.posToVal(u.select.left, 'x') : u.posToVal(u.select.top + u.select.height, 'x'),
          to: isXAxisHorizontal ? u.posToVal(u.select.left + u.select.width, 'x') : u.posToVal(u.select.top, 'x'),
        });

        console.log('[TooltipPlugin2::setSelect] renderTooltip');
        renderTooltip(u, true);
      }
    }

    // manually hide selected region (since cursor.drag.setScale = false)
    u.setSelect({ left: 0, width: 0, top: 0, height: 0 }, false);
  });
  usePlotConfigHook(config, 'setData', (u) => {
    yZoomed = false;
    yDrag = false;

    if (isPinned) {
      dismiss(u);
    }
  });

  // e.g. to highlight the hovered/closest series
  // TODO: we only need this for multi/all mode?

  // fires on series focus/proximity changes
  usePlotConfigHook(config, 'setSeries', (u, seriesIdx) => {
    closestSeriesIdx = seriesIdx;

    viaSync = u.cursor.event == null;
    const hovering = isUserHovering();
    if (hovering) {
      console.log('[TooltipPlugin2::setSeries] renderTooltip');
      renderTooltip(u);
    }
  });
  usePlotConfigHook(config, 'ready', updatePlotVisible);

  // fires on mousemoves
  usePlotConfigHook(config, 'setCursor', (u) => {
    if (!sizeRef.current) {
      console.warn('[TooltipPlugin2::setCursor] sizeRef not defined!');
      return;
    }

    viaSync = u.cursor.event == null;

    if (!isHovering) {
      return;
    }

    let { left = -10, top = -10 } = u.cursor;

    if (left >= 0 || top >= 0) {
      let clientX = u.rect.left + left;
      let clientY = u.rect.top + top;

      let transform = '';

      let { width, height } = sizeRef.current;

      width += TOOLTIP_OFFSET;
      height += TOOLTIP_OFFSET;

      if (offsetY !== 0) {
        if (clientY + height < winHgt || clientY - height < 0) {
          offsetY = 0;
        } else if (offsetY !== -height) {
          offsetY = -height;
        }
      } else {
        if (clientY + height > winHgt && clientY - height >= 0) {
          offsetY = -height;
        }
      }

      if (offsetX !== 0) {
        if (clientX + width < winWid || clientX - width < 0) {
          offsetX = 0;
        } else if (offsetX !== -width) {
          offsetX = -width;
        }
      } else {
        if (clientX + width > winWid && clientX - width >= 0) {
          offsetX = -width;
        }
      }

      const shiftX = clientX + (offsetX === 0 ? TOOLTIP_OFFSET : -TOOLTIP_OFFSET);
      const shiftY = clientY + (offsetY === 0 ? TOOLTIP_OFFSET : -TOOLTIP_OFFSET);

      const reflectX = offsetX === 0 ? '' : 'translateX(-100%)';
      const reflectY = offsetY === 0 ? '' : 'translateY(-100%)';

      // TODO: to a transition only when switching sides
      // transition: transform 100ms;

      transform = `translateX(${shiftX}px) ${reflectX} translateY(${shiftY}px) ${reflectY}`;

      // @todo fix mutating domRef styles
      if (domRef.current != null) {
        domRef.current.style.transform = transform;
      } else {
        console.warn('[TooltipPlugin2::setCursor] renderTooltip');
        setStyle({ ...style, transform });
        renderTooltip(u);
      }
    }
  });

  useLayoutEffect(() => {
    sizeRef.current?.observer.disconnect();

    if (!plot) {
      // @todo remove debug
      console.warn('[useLayoutEffect::config] Plot not initiated!');
      return;
    }

    console.log('[useLayoutEffect::config] Plot initiated!');

    sizeRef.current = {
      width: 0,
      height: 0,
      observer: new ResizeObserver((entries) => {
        let size = sizeRef.current!;

        for (const entry of entries) {
          if (entry.borderBoxSize?.length > 0) {
            size.width = entry.borderBoxSize[0].inlineSize;
            size.height = entry.borderBoxSize[0].blockSize;
          } else {
            size.width = entry.contentRect.width;
            size.height = entry.contentRect.height;
          }
        }
      }),
    };

    if (clientZoom || queryZoom != null) {
      config.setCursor({
        bind: {
          dblclick: (u) => () => {
            if (!maybeZoomAction(u.cursor!.event)) {
              return null;
            }

            if (clientZoom && yZoomed) {
              for (let key in u.scales!) {
                if (key !== 'x') {
                  // @ts-ignore (this is not typed correctly in uPlot, assigning nulls means auto-scale / reset)
                  u.setScale(key, { min: null, max: null });
                }
              }

              yZoomed = false;
            } else if (queryZoom != null) {
              let xScale = u.scales.x;

              const frTs = xScale.min!;
              const toTs = xScale.max!;
              const pad = (toTs - frTs) / 2;

              queryZoom({ from: frTs - pad, to: toTs + pad });
            }

            return null;
          },
        },
      });
    }

    const onscroll = (e: Event) => {
      updatePlotVisible(plot);
      isHovering && e.target instanceof Node && e.target.contains(plot!.root) && dismiss(plot);
    };

    window.addEventListener('resize', () => updateWinSize(plot));
    window.addEventListener('scroll', onscroll, true);

    return () => {
      sizeRef.current?.observer.disconnect();

      //@todo does this clean up?
      window.removeEventListener('resize', () => updateWinSize(plot));
      window.removeEventListener('scroll', onscroll, true);

      // in case this component unmounts while anchored (due to data auto-refresh + re-config)
      document.removeEventListener('mousedown', downEventOutside, true);
      document.removeEventListener('keydown', downEventOutside, true);
    };
  }, [config, plot]);

  useLayoutEffect(() => {
    const size = sizeRef.current;

    if (!size) {
      console.warn('[useLayoutEffect::hover] sizeRef not defined!');
      return;
    }

    size.width = 0;
    size.height = 0;

    if (!domRef.current) {
      console.warn('[useLayoutEffect::hover] domRef not defined!');
      return;
    }
    size.observer.disconnect();
    size.observer.observe(domRef.current);

    // since the above observer is attached after container is in DOM, we need to manually update sizeRef
    // and re-trigger a cursor move to do initial positioning math
    const { width, height } = domRef.current.getBoundingClientRect();
    size.width = width;
    size.height = height;

    let event = plot?.cursor.event;

    // if not viaSync, re-dispatch real event
    if (event != null) {
      // we expect to re-dispatch mousemove, but may have a different event type, so create a mousemove event and fire that instead
      // this doesn't work for every mobile device, so fall back to checking the useragent as well
      const isMobile = event.type !== 'mousemove' || userAgentIsMobile;

      if (isMobile) {
        event = new MouseEvent('mousemove', {
          view: window,
          bubbles: true,
          cancelable: true,
          clientX: event.clientX,
          clientY: event.clientY,
          screenX: event.screenX,
          screenY: event.screenY,
        });
      }

      // this works around the fact that uPlot does not unset cursor.event (for perf reasons)
      // so if the last real mouse event was mouseleave and you manually trigger u.setCursor()
      // it would end up re-dispatching mouseleave
      const isStaleEvent = isMobile ? false : performance.now() - event.timeStamp > 16;

      !isStaleEvent && plot?.over.dispatchEvent(event);
    } else {
      plot?.setCursor(
        {
          left: plot.cursor.left!,
          top: plot.cursor.top!,
        },
        true
      );
    }
  }, [isHovering]);

  const isRenderingTooltip = plot && isHovering;
  console.log('[TooltipPlugin2::RENDER]', { annotationRange, isPinned, isHovering, isRenderingTooltip });

  if (isRenderingTooltip) {
    return createPortal(
      <div
        className={cx(styles.tooltipWrapper, isPinned && styles.pinned)}
        style={style}
        aria-live="polite"
        aria-atomic="true"
        ref={domRef}
      >
        {isPinned && <CloseButton onClick={() => dismiss(plot)} />}
        {tooltipContent}
      </div>,
      portalRoot.current
    );
  }

  return null;
};

const getStyles = (theme: GrafanaTheme2, maxWidth?: number) => ({
  tooltipWrapper: css({
    top: 0,
    left: 0,
    zIndex: theme.zIndex.tooltip,
    whiteSpace: 'pre',
    borderRadius: theme.shape.radius.default,
    position: 'fixed',
    background: theme.colors.background.elevated,
    border: `1px solid ${theme.colors.border.weak}`,
    boxShadow: theme.shadows.z2,
    userSelect: 'text',
    maxWidth: maxWidth ?? 'none',
  }),
  pinned: css({
    boxShadow: theme.shadows.z3,
  }),
});
