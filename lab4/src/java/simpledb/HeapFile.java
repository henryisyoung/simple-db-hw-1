package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private TupleDesc td;
	private File file;
	private int tableid;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.file = f;
		this.td = td;
		tableid=file.getAbsoluteFile().hashCode();
	}

	// a hack to remember the last page that had a free slot
	private volatile int lastEmptyPage = -1;

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return file;
	}

	
	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		
		return file.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		// some code goes here
		HeapPage resHP;
		try {
			// HeapPage resHP;
			RandomAccessFile rfile = new RandomAccessFile(file, "r");
			int psize = BufferPool.getPageSize();
			int indice = pid.pageNumber() * psize;
			byte[] readdata = new byte[psize];

			// get data
			rfile.seek(indice);
			rfile.read(readdata, 0, psize);
			rfile.close();
			resHP = new HeapPage((HeapPageId) pid, readdata);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new NoSuchElementException();
		}
		return resHP;
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for lab1
		HeapPage p = (HeapPage) page;
		// System.out.println("Writing back page " + p.getId().pageno());
		byte[] data = p.getPageData();
		RandomAccessFile rf = new RandomAccessFile(file, "rw");
		rf.seek(p.getId().pageNumber() * BufferPool.getPageSize());
		rf.write(data);
		rf.close();
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {

		return (int) Math.ceil((double) file.length() / BufferPool.getPageSize());
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here

		// not necessary for lab1
		ArrayList<Page> dirtypages = new ArrayList<Page>();

		// find the first page with a free slot in it
		int i = 0;
		if (lastEmptyPage != -1)
			i = lastEmptyPage;
		// XXX: Would it not be better to scan from numPages() to 0 since the
		// last pages are more likely to have empty slots?
		for (; i < numPages(); i++) {
			Debug.log(4, "HeapFile.addTuple: checking free slots on page %d of table %d", i, tableid);
			HeapPageId pid = new HeapPageId(tableid, i);
			HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

			// no empty slots
			//
			// think about why we have to invoke releasePage here.
			// can you think of ways where
			if (p.getNumEmptySlots() == 0) {
				Debug.log(4, "HeapFile.addTuple: no free slots on page %d of table %d", i, tableid);

				// we mistakenly got here through lastEmptyPage, just add a page
				// XXX we know this isn't very pretty.
				if (lastEmptyPage != -1) {
					lastEmptyPage = -1;
					break;
				}
				continue;
			}
			Debug.log(4, "HeapFile.addTuple: %d free slots in table %d", p.getNumEmptySlots(), tableid);
			p.insertTuple(t);
			lastEmptyPage = p.getId().pageNumber();
			// System.out.println("nfetches = " + nfetches);
			dirtypages.add(p);
			return dirtypages;
		}
		synchronized (this) {
			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true));
			byte[] emptyData = HeapPage.createEmptyPageData();
			bw.write(emptyData);
			bw.close();
		}

		// by virtue of writing these bits to the HeapFile, it is now visible.
		// so some other dude may have obtained a read lock on the empty page
		// we just created---which is ok, we haven't yet added the tuple.
		// we just need to lock the page before we can add the tuple to it.

		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableid, numPages() - 1),
				Permissions.READ_WRITE);
		p.insertTuple(t);
		lastEmptyPage = p.getId().pageNumber();
		// System.out.println("nfetches = " + nfetches);
		dirtypages.add(p);
		return dirtypages;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
				new HeapPageId(tableid, t.getRecordId().getPageId().pageNumber()), Permissions.READ_WRITE);
		p.deleteTuple(t);
		ArrayList<Page> pages = new ArrayList<Page>();
		pages.add(p);
		return pages;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		return new HeapFileIterator(this, tid);
	}
}
/*
 * private class HeapFileIterator implements DbFileIterator {
 * 
 * private Iterator<Tuple> hfiterator=null; private HeapFile heapfile=null;
 * private TupleDesc td; private TransactionId tid; private HeapPage currentpg;
 * private int currentpgno;
 * 
 * public HeapFileIterator(HeapFile heapfile,TransactionId tid) {
 * this.heapfile=heapfile; this.tid=tid; currentpgno=0; }
 * 
 * @Override public void open() throws DbException, TransactionAbortedException
 * { // TODO Auto-generated method stub updatepage(currentpgno); }
 * 
 * 
 * public void updatepage(int currentpgno) throws TransactionAbortedException,
 * DbException{ HeapPageId tempHPId=new
 * HeapPageId(heapfile.getId(),currentpgno);
 * 
 * currentpg=(HeapPage) Database.getBufferPool().getPage(tid,
 * tempHPId,Permissions.READ_ONLY ); hfiterator=currentpg.iterator();
 * 
 * }
 * 
 * @Override public boolean hasNext() throws DbException,
 * TransactionAbortedException { // TODO Auto-generated method stub
 * if(hfiterator==null) return false;
 * 
 * return hfiterator.hasNext();
 * 
 * 
 * }
 * 
 * @Override public Tuple next() throws DbException,
 * TransactionAbortedException, NoSuchElementException { // TODO Auto-generated
 * method stub
 * 
 * if(hfiterator==null) throw new NoSuchElementException();
 * if(!hfiterator.hasNext()) throw new NoSuchElementException();
 * if(hfiterator.hasNext()) { if (currentpgno < heapfile.numPages()-1) {
 * currentpgno++; updatepage(currentpgno);
 * 
 * } }
 * 
 * return hfiterator.next(); }
 * 
 * @Override public void rewind() throws DbException,
 * TransactionAbortedException { // TODO Auto-generated method stub close();
 * open();
 * 
 * }
 * 
 * @Override public void close() { // TODO Auto-generated method stub
 * 
 * hfiterator=null;
 * 
 * currentpgno=0; heapfile=null; }
 * 
 * } }
 */
