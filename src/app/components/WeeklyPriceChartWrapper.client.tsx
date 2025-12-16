"use client";

import React from 'react';
import WeeklyPriceChart from '@/components/WeeklyPriceChart.client';

type Props = {
  history?: Array<{ t: string; p: number }>;
};

export default function WeeklyPriceChartWrapper({ history = [] }: Props) {
  return <WeeklyPriceChart history={history} />;
}
