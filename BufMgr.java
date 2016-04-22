// CS 587
// Name: Sunil Shenoy

package bufmgr;

import java.util.*;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3> The buffer manager manages an array of main
 * memory pages. The array is called the buffer pool, each page is called a
 * frame. It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and relational
 * operators.
 */

public class BufMgr implements GlobalConst {

	Frame[] frametab;
	HashMap<Integer, Integer> frame_Map;
	int numframes;
	int current = 0;

	/**
	 * Constructs a buffer manager by initializing member data.
	 * 
	 * @param numframes
	 *            number of frames in the buffer pool
	 */
	public BufMgr(int numframes) {

		this.numframes = numframes;
		frametab = new Frame[numframes];
		Arrays.fill(frametab, new Frame());
		frame_Map = new HashMap<Integer, Integer>();

	}

	/**
	 * The result of this call is that disk page number pageno should reside in
	 * a frame in the buffer pool and have an additional pin assigned to it, and
	 * mempage should refer to the contents of that frame. <br>
	 * <br>
	 * 
	 * If disk page pageno is already in the buffer pool, this simply increments
	 * the pin count. Otherwise, this<br>
	 * 
	 * <pre>
	 * 	uses the replacement policy to select a frame to replace
	 * 	writes the frame's contents to disk if valid and dirty
	 * 	if (contents == PIN_DISKIO)
	 * 		read disk page pageno into chosen frame
	 * 	else (contents == PIN_MEMCPY)
	 * 		copy mempage into chosen frame
	 * 	[omitted from the above is maintenance of the frame table and hash map]
	 * </pre>
	 * 
	 * @param pageno
	 *            identifies the page to pin
	 * @param mempage
	 *            An output parameter referring to the chosen frame. If
	 *            contents==PIN_MEMCPY it is also an input parameter which is
	 *            copied into the chosen frame, see the contents parameter.
	 * @param contents
	 *            Describes how the contents of the frame are determined.<br>
	 *            If PIN_DISKIO, read the page from disk into the frame.<br>
	 *            If PIN_MEMCPY, copy mempage into the frame.<br>
	 *            If PIN_NOOP, copy nothing into the frame - the frame contents
	 *            are irrelevant.<br>
	 *            Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is
	 *            avoided.
	 * @throws IllegalArgumentException
	 *             if PIN_MEMCPY and the page is pinned.
	 * @throws IllegalStateException
	 *             if all pages are pinned (i.e. pool is full)
	 */
	public void pinPage(PageId pageno, Page mempage, int contents) {

		Integer frameno;
		// check if page is in buffer
		if ((frameno = frame_Map.get(pageno.pid)) != null) {
			// If so then increment its pin count
			frametab[frameno].inc_pinCount();
		}

		else {
			// page is not in buffer
			// so pick a frame to allocate it into
			// first search for empty frames
			frameno = null;
			for (Integer f = 0; f < numframes; f++) {
				if (!frametab[f].valid_Page) {
					frameno = f;
					break;
				}
			}

			// if none available then look for victim frames to displace
			if (frameno == null)
				if ((frameno = pickVictim()) == null)
					throw new IllegalStateException(
							"all pages are pinned (i.e. pool is full)");

			// flush the victim page from the buffer and remove it from the map
			if (frametab[frameno].valid_Page) {
				flushPage(frametab[frameno].page_Number);
				frame_Map.remove(frametab[frameno].page_Number.pid);
			}

			// create a new frame for the disk page to be pinned and set
			// its valid bit, pageId and pin count
			frametab[frameno] = new Frame();
			frametab[frameno].set_valid_Page(true);
			frametab[frameno].set_page_Number(new PageId(pageno.pid));
			frametab[frameno].inc_pinCount();

			// update frame's contents from memory or disk
			if (contents == PIN_MEMCPY) {
				frametab[frameno].copyPage(mempage);
			} else if (contents == PIN_DISKIO) {
				Minibase.DiskManager.read_page(pageno, frametab[frameno]);
			}

			// update the map
			frame_Map.put(pageno.pid, frameno);

		}

		// point mempage to this frame's data
		mempage.setData(frametab[frameno].getData());

	}

	/**
	 * Unpins a disk page from the buffer pool, decreasing its pin count.
	 * 
	 * @param pageno
	 *            identifies the page to unpin
	 * @param dirty
	 *            UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
	 * @throws IllegalArgumentException
	 *             if the page is not in the buffer pool or not pinned
	 */
	public void unpinPage(PageId pageno, boolean dirty) {

		Integer frameno;
		if ((frameno = frame_Map.get(pageno.pid)) == null
				|| frametab[frameno].not_pinned()) {
			throw new IllegalArgumentException(
					"The page to Unpin is not in the buffer pool or not pinned");
		}

		// decrement pin count and if it is zero then set reference bit to
		// support the Clock replacement algorithm
		else {
			frametab[frameno].dec_pinCount();
			if (frametab[frameno].not_pinned())
				frametab[frameno].set_referenced(true);
			frametab[frameno].set_dirty(dirty);
		}

	}

	/**
	 * Allocates a run of new disk pages and pins the first one in the buffer
	 * pool. The pin will be made using PIN_MEMCPY. Watch out for disk page
	 * leaks.
	 * 
	 * @param firstpg
	 *            input and output: holds the contents of the first allocated
	 *            page and refers to the frame where it resides
	 * @param run_size
	 *            input: number of pages to allocate
	 * @return page id of the first allocated page
	 * @throws IllegalArgumentException
	 *             if firstpg is already pinned
	 * @throws IllegalStateException
	 *             if all pages are pinned (i.e. pool exceeded)
	 */
	public PageId newPage(Page firstpg, int run_size) {

		PageId firstpgid;
		// first allocate the pages contiguously on disk
		firstpgid = Minibase.DiskManager.allocate_page(run_size);
		if (frame_Map.get(firstpgid.pid) != null
				&& !frametab[frame_Map.get(firstpgid.pid)].not_pinned()) {
			throw new IllegalArgumentException("firstpg is already pinned");
		} else
			// then pin the first page of the run
			pinPage(firstpgid, firstpg, PIN_MEMCPY);
		return (firstpgid);

	}

	/**
	 * Deallocates a single page from disk, freeing it from the pool if needed.
	 * 
	 * @param pageno
	 *            identifies the page to remove
	 * @throws IllegalArgumentException
	 *             if the page is pinned
	 */
	public void freePage(PageId pageno) {

		Integer frameno;
		if ((frameno = frame_Map.get(pageno.pid)) != null
				&& !frametab[frameno].not_pinned()) {
			throw new IllegalArgumentException(
					"page to be deallocated is pinned");
		} else {
			Minibase.DiskManager.deallocate_page(pageno);
			if (frame_Map.get(pageno.pid) != null) {
				frametab[frame_Map.get(pageno.pid)] = new Frame();
				frame_Map.remove(pageno.pid);
			}
		}
	}

	/**
	 * Write all valid and dirty frames to disk. Note flushing involves only
	 * writing, not unpinning or freeing or the like.
	 * 
	 */
	public void flushAllFrames() {

		for (Integer frameno : frame_Map.values()) {
			if (frametab[frameno].valid_Page)
				flushPage(frametab[frameno].page_Number);
		}

	}

	/**
	 * Write a page in the buffer pool to disk, if dirty.
	 * 
	 * @throws IllegalArgumentException
	 *             if the page is not in the buffer pool
	 */
	public void flushPage(PageId pageno) {
		Integer frameno;
		if ((frameno = frame_Map.get(pageno.pid)) == null)
			throw new IllegalArgumentException(
					"Page to be flushed is not in the buffer pool " + frameno
							+ " " + pageno.pid);
		else if ((frametab[frameno].dirty) && (frametab[frameno].valid_Page)) {
			Minibase.DiskManager.write_page(pageno, frametab[frameno]);
			frametab[frameno].set_dirty(false);
		}

	}

	/**
	 * Gets the total number of buffer frames.
	 */
	public int getNumFrames() {

		return numframes;

	}

	/**
	 * Gets the total number of unpinned buffer frames.
	 */
	public int getNumUnpinned() {

		int numUnPinned = 0;
		for (int f = 0; f < numframes; f++) {
			if (!frametab[f].valid_Page || frametab[f].not_pinned())
				numUnPinned++;
		}

		return numUnPinned;

	}

	private Integer pickVictim() {
		// start from where the clock hand is remembering that position
		int current_ptr = current;

		// maximum go twice around the clock before giving up
		for (int loops = 0; loops < 2; loops++) {
			do {
				// pick first unpinned page encountered whose referenced bit =
				// false as the victim
				if (frametab[current].not_pinned()) {
					if (!frametab[current].referenced) {
						return current;
					} else
						// clear unpinned page's referenced bit if set so that
						// it is a candidate for victim next time unless it gets
						// pinned meanwhile
						frametab[current].set_referenced(false);
				}
				current = (current + 1) % numframes;
			} while (current != current_ptr);
		}
		return null;
	}

	// for debug purposes
	public void printframes() {
		System.out.println("Printing buffer contents:");
		for (int f = 0; f < numframes; f++) {
			Frame frame = frametab[f];
			if (frame.valid_Page)
				System.out.println(f + "\t" + frame.valid_Page + "\t"
						+ frame.page_Number.pid + "\t" + frame.pinCount);
			else
				System.out.println(f + "\t" + frame.valid_Page);
		}
		printmap();
		System.out.println("**********");
	}

	public void printmap() {
		System.out.println("Printing map contents:");
		for (Integer i : frame_Map.keySet()) {
			System.out.println(i + "\t" + frame_Map.get(i));
		}
	}
}