import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { goldenManifest } from '../test/fixture.js';
import { YAxis } from './YAxis.js';

describe('YAxis', () => {
  it('keeps endpoint tick lines on canonical raster coordinates while labels remain legible', () => {
    const { container } = render(
      <YAxis scale={goldenManifest().layout.y_scale} renderedHeight={50} />,
    );
    const zero = container.querySelector('[data-tick-value="0"]')!;
    const maximum = container.querySelector('[data-tick-value="350"]')!;
    expect(zero.querySelector('line')).toHaveAttribute('y1', '50');
    expect(zero.querySelector('text')).toHaveAttribute('y', '47');
    expect(maximum.querySelector('line')).toHaveAttribute('y1', '0');
    expect(maximum.querySelector('text')).toHaveAttribute('y', '7');
  });
});
