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

  it('deconflicts canonical 300 and 350 labels without moving their 400px tick lines', () => {
    const { container } = render(
      <YAxis scale={goldenManifest().layout.y_scale} renderedHeight={400} />,
    );
    const threeHundred = container.querySelector('[data-tick-value="300"]')!;
    const maximum = container.querySelector('[data-tick-value="350"]')!;

    expect(threeHundred.querySelector('line')).toHaveAttribute('y1', '6.937185430455145');
    expect(maximum.querySelector('line')).toHaveAttribute('y1', '0');
    expect(threeHundred).toHaveAttribute('data-label-visible', 'false');
    expect(threeHundred.querySelector('text')).toBeNull();
    expect(maximum).toHaveAttribute('data-label-visible', 'true');
    expect(maximum.querySelector('text')).toHaveAttribute('y', '7');

    const labelCenters = [...container.querySelectorAll('[data-label-visible="true"] text')]
      .map((label) => Number(label.getAttribute('y')))
      .sort((a, b) => a - b);
    expect(labelCenters.every((center, index) => (
      index === 0 || center - labelCenters[index - 1] >= 12
    ))).toBe(true);
  });
});
