import { fireEvent, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { AssociationPointV1 } from '../contracts/types.js';
import { goldenManifest } from '../test/fixture.js';
import {
  AssociationHitTable,
  type AssociationHitTableControlledProps,
} from './AssociationHitTable.js';

function dataRows(): HTMLElement[] {
  return within(screen.getByRole('table')).getAllByRole('row').slice(1).filter((row) => !row.getAttribute('aria-hidden'));
}

function controlledProps(
  overrides: Partial<AssociationHitTableControlledProps> = {},
): AssociationHitTableControlledProps {
  return {
    mode: 'server',
    rows: goldenManifest().top_hits,
    total: 202,
    offset: 100,
    limit: 100,
    query: '',
    sort: 'pvalue',
    direction: 'ascending',
    loading: false,
    onQueryChange: vi.fn(),
    onSortChange: vi.fn(),
    onOffsetChange: vi.fn(),
    ...overrides,
  };
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

  it('renders a controlled server page exactly as received without local filtering or sorting', () => {
    const rows = [...goldenManifest().top_hits].reverse();
    render(<AssociationHitTable {...controlledProps({
      rows,
      query: 'does-not-match-any-row',
      sort: 'locus',
      direction: 'ascending',
    })} />);

    expect(screen.getByRole('searchbox', { name: 'Search hits' })).toHaveValue('does-not-match-any-row');
    expect(dataRows()).toHaveLength(2);
    expect(dataRows()[0]).toHaveTextContent('2:9');
    expect(dataRows()[1]).toHaveTextContent('1:3 A>G');
  });

  it('delegates query and sort changes and explicitly resets the offset', async () => {
    const user = userEvent.setup();
    const onQueryChange = vi.fn();
    const onSortChange = vi.fn();
    const onOffsetChange = vi.fn();
    render(<AssociationHitTable {...controlledProps({
      onQueryChange,
      onSortChange,
      onOffsetChange,
    })} />);

    await user.type(screen.getByRole('searchbox', { name: 'Search hits' }), 'G');
    expect(onQueryChange).toHaveBeenCalledWith('G');
    expect(onOffsetChange).toHaveBeenCalledWith(0);

    onOffsetChange.mockClear();
    await user.click(screen.getByRole('button', { name: 'Variant' }));
    expect(onSortChange).toHaveBeenCalledWith('locus', 'ascending');
    expect(onOffsetChange).toHaveBeenCalledWith(0);

    onOffsetChange.mockClear();
    await user.click(screen.getByRole('button', { name: 'P-value' }));
    expect(onSortChange).toHaveBeenCalledWith('pvalue', 'descending');
    expect(onOffsetChange).toHaveBeenCalledWith(0);
  });

  it('shows the server range and delegates bounded previous and next pages', async () => {
    const user = userEvent.setup();
    const onOffsetChange = vi.fn();
    const { rerender } = render(<AssociationHitTable {...controlledProps({ onOffsetChange })} />);

    expect(screen.getByRole('status')).toHaveTextContent('101–102 of 202 hits');
    await user.click(screen.getByRole('button', { name: 'Previous' }));
    expect(onOffsetChange).toHaveBeenCalledWith(0);
    await user.click(screen.getByRole('button', { name: 'Next' }));
    expect(onOffsetChange).toHaveBeenCalledWith(200);

    rerender(<AssociationHitTable {...controlledProps({ offset: 0, onOffsetChange })} />);
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
    rerender(<AssociationHitTable {...controlledProps({ offset: 200, total: 202, onOffsetChange })} />);
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled();
  });

  it('announces loading and errors without discarding already loaded rows', () => {
    const { rerender } = render(<AssociationHitTable {...controlledProps({ loading: true })} />);
    expect(screen.getByRole('region', { name: 'Top association hits' })).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByText('Loading association hits.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
    expect(dataRows()).toHaveLength(2);

    rerender(<AssociationHitTable {...controlledProps({ error: 'The table page could not be loaded.' })} />);
    expect(screen.getByRole('alert')).toHaveTextContent('The table page could not be loaded.');
    expect(dataRows()).toHaveLength(2);
  });

  it('keeps an off-page controlled selection while preserving row focus and keyboard behavior', () => {
    const onSelectedHitChange = vi.fn();
    const offPageKey = '22:999:A:T:9000';
    const { rerender } = render(<AssociationHitTable {...controlledProps({
      selectedHitKey: offPageKey,
      onSelectedHitChange,
    })} />);

    expect(dataRows().every((row) => row.getAttribute('aria-selected') === 'false')).toBe(true);
    expect(dataRows()[0]).toHaveAttribute('tabindex', '0');
    expect(onSelectedHitChange).not.toHaveBeenCalled();
    dataRows()[0].focus();
    fireEvent.keyDown(dataRows()[0], { key: 'ArrowDown' });
    expect(onSelectedHitChange).toHaveBeenCalledWith(expect.objectContaining({ contig: '2' }));

    rerender(<AssociationHitTable {...controlledProps({ selectedHitKey: '2:9:::2' })} />);
    expect(dataRows()[1]).toHaveAttribute('aria-selected', 'true');
  });

  it('retains virtualization for controlled server pages', () => {
    const template = goldenManifest().top_hits[0];
    const rows: AssociationPointV1[] = Array.from({ length: 500 }, (_, index) => ({
      ...template,
      position: index + 1,
      source_ordinal: index,
    }));
    const { container } = render(<AssociationHitTable {...controlledProps({
      rows,
      total: 500,
      offset: 0,
      limit: 500,
      height: 120,
      rowHeight: 30,
    })} />);
    expect(dataRows().length).toBeLessThan(20);
    const viewport = container.querySelector('.association-table-viewport') as HTMLDivElement;
    fireEvent.scroll(viewport, { target: { scrollTop: 3000 } });
    expect(dataRows().some((row) => row.textContent?.includes('1:101'))).toBe(true);
  });
});
