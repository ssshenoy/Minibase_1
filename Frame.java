// CS 587
// Name: Sunil Shenoy

package bufmgr;

import global.*;

public class Frame extends Page {

	int pinCount;
	boolean valid_Page;
	boolean dirty;
	PageId page_Number;
	boolean referenced;

	public Frame() {
	    super();
	    pinCount = 0;
	    dirty = false;
	    valid_Page = false;
	    page_Number = null;
	    referenced = false;
	  }
	
	public void inc_pinCount () {
		this.pinCount++;
	}
	
	public void dec_pinCount () {
		this.pinCount--;
	}
	
	public boolean not_pinned () {
		return (pinCount == 0);
	}
	
	public void set_dirty (boolean dirty) {
		this.dirty = dirty;
	}
	
	public void set_referenced (boolean referenced) {
		this.referenced = referenced;
	}
	
	public void set_valid_Page (boolean valid_Page) {
		this.valid_Page = valid_Page;
	}
	
	public void set_page_Number (PageId pid) {
		this.page_Number = pid;
	}
}
