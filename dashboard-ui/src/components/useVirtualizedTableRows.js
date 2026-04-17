import { useEffect, useRef, useState } from "react";

const DEFAULT_VIEWPORT_HEIGHT = 520;

function getViewportWidth() {
  if (typeof window === "undefined") {
    return 1400;
  }
  return window.innerWidth || 1400;
}

export function useVirtualizedTableRows({
  rowCount,
  rowHeight = 72,
  overscan = 8,
  minRowsToVirtualize = 45,
  minViewportWidth = 980,
}) {
  const containerRef = useRef(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(DEFAULT_VIEWPORT_HEIGHT);
  const [viewportWidth, setViewportWidth] = useState(getViewportWidth);

  useEffect(() => {
    function handleResize() {
      setViewportWidth(getViewportWidth());
    }

    if (typeof window !== "undefined") {
      window.addEventListener("resize", handleResize);
      return () => window.removeEventListener("resize", handleResize);
    }

    return undefined;
  }, []);

  useEffect(() => {
    const node = containerRef.current;
    if (!node) {
      return undefined;
    }

    function measure() {
      setViewportHeight(node.clientHeight || DEFAULT_VIEWPORT_HEIGHT);
    }

    measure();

    if (typeof ResizeObserver !== "undefined") {
      const observer = new ResizeObserver(measure);
      observer.observe(node);
      return () => observer.disconnect();
    }

    if (typeof window !== "undefined") {
      window.addEventListener("resize", measure);
      return () => window.removeEventListener("resize", measure);
    }

    return undefined;
  }, [rowCount]);

  const virtualizationEnabled = rowCount >= minRowsToVirtualize && viewportWidth >= minViewportWidth;

  const visibleCount = Math.max(1, Math.ceil(viewportHeight / rowHeight) + overscan * 2);
  const startIndex = virtualizationEnabled ? Math.max(0, Math.floor(scrollTop / rowHeight) - overscan) : 0;
  const endIndex = virtualizationEnabled ? Math.min(rowCount, startIndex + visibleCount) : rowCount;
  const topPadding = virtualizationEnabled ? startIndex * rowHeight : 0;
  const bottomPadding = virtualizationEnabled ? Math.max(0, (rowCount - endIndex) * rowHeight) : 0;

  function handleScroll(event) {
    if (!virtualizationEnabled) {
      return;
    }
    setScrollTop(event.currentTarget.scrollTop || 0);
  }

  return {
    containerRef,
    handleScroll,
    virtualizationEnabled,
    startIndex,
    endIndex,
    topPadding,
    bottomPadding,
  };
}
