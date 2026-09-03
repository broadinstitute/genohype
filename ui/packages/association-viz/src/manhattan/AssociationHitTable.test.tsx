import { fireEvent, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { AssociationPointV1 } from '../contracts/types.js';
import { goldenManifest } from '../test/fixture.js';
import { AssociationHitTable } from './AssociationHitTable.js';

function dataRows(): HTMLElement[] {
  return within(screen.getByRole('table')).getAllByRole('row').slice(1).filter((row) => !row.getAttribute('aria-hidden'));
}

describe('AssociationHitTable', () => {
  it('searches and sorts bounded top hits', async () => {
    const user = userEvent.setup();
    render(<AssociationHitTable hits={goldenManifest().top_hits} />);
    expect(dataRows()[0]).toHaveTextContent('1:3 A>G');
    await user.click(screen.getByRole('button', { name: 'Variant' }));
    await user.click(screen.getByRole('button', { name: 'Variant' }));
    expect(dataRows()[0]).toHaveTextContent('2:9');
    await user.type(screen.getByRole('searchbox', { name: 'Search hits' }), 'GENE1');
    expect(dataRows()).toHaveLength(1);
    expect(dataRows()[0]).toHaveTextContent('GENE1');
    expect(screen.getByRole('status')).toHaveTextContent('1 of 2');
  });

  it('supports roving keyboard selection, controlled selection, and open-locus actions', async () => {
    const user = userEvent.setup();
    const onSelectedHitChange = vi.fn();
    const onOpenLocus = vi.fn();
    const { rerender } = render(
      <AssociationHitTable
        hits={goldenManifest().top_hits}
        onSelectedHitChange={onSelectedHitChange}
        onOpenLocus={onOpenLocus}
      />,
    );
    const first = dataRows()[0];
    first.focus();
    fireEvent.keyDown(first, { key: 'ArrowDown' });
    expect(onSelectedHitChange).toHaveBeenCalledWith(expect.objectContaining({ contig: '2', position: 9 }));
    rerender(
      <AssociationHitTable
        hits={goldenManifest().top_hits}
        selectedHitKey="2:9:::2"
        onSelectedHitChange={onSelectedHitChange}
        onOpenLocus={onOpenLocus}
      />,
    );
    expect(dataRows()[1]).toHaveAttribute('aria-selected', 'true');
    fireEvent.keyDown(dataRows()[1], { key: 'o' });
    expect(onOpenLocus).toHaveBeenCalledWith(expect.objectContaining({ contig: '2' }));
    const openButton = within(dataRows()[1]).getByRole('button', { name: /Open locus/ });
    fireEvent.click(openButton);
    expect(onOpenLocus).toHaveBeenCalledTimes(2);
    const selectionsBeforeButtonKeys = onSelectedHitChange.mock.calls.length;
    openButton.focus();
    await user.keyboard('{Enter} ');
    expect(onOpenLocus).toHaveBeenCalledTimes(4);
    expect(onSelectedHitChange).toHaveBeenCalledTimes(selectionsBeforeButtonKeys);
  });

  it('moves keyboard focus and the virtual window across a render boundary', () => {
    const template = goldenManifest().top_hits[0];
    const hits: AssociationPointV1[] = Array.from({ length: 30 }, (_, index) => ({
      ...template,
      position: index + 1,
      pvalue: (index + 1) / 1000,
      neg_log10_p: -Math.log10((index + 1) / 1000),
      source_ordinal: index,
    }));
    const { container } = render(<AssociationHitTable hits={hits} height={60} rowHeight={30} />);
    dataRows()[0].focus();
    for (let index = 0; index < 8; index += 1) {
      fireEvent.keyDown(document.activeElement as HTMLElement, { key: 'ArrowDown' });
    }
    expect(document.activeElement).toHaveTextContent('1:9');
    expect((container.querySelector('.association-table-viewport') as HTMLDivElement).scrollTop).toBeGreaterThan(0);
  });

  it('virtualizes large bounded hit lists', () => {
    const template = goldenManifest().top_hits[0];
    const hits: AssociationPointV1[] = Array.from({ length: 500 }, (_, index) => ({
      ...template,
      position: index + 1,
      pvalue: (index + 1) / 1000,
      neg_log10_p: -Math.log10((index + 1) / 1000),
      source_ordinal: index,
    }));
    const { container } = render(<AssociationHitTable hits={hits} height={120} rowHeight={30} />);
    expect(dataRows().length).toBeLessThan(20);
    const viewport = container.querySelector('.association-table-viewport') as HTMLDivElement;
    fireEvent.scroll(viewport, { target: { scrollTop: 3000 } });
    expect(dataRows().some((row) => row.textContent?.includes('1:101'))).toBe(true);
  });
});
