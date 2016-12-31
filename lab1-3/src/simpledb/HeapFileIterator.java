package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{

	private Iterator<Tuple> hfiterator=null;
	private HeapFile heapfile=null;
	private TupleDesc td;
	private TransactionId tid;
	private HeapPage currentpg;
	private int currentpgno;
	
	public HeapFileIterator(HeapFile heapfile,TransactionId tid)
	{
		this.heapfile=heapfile;
		this.tid=tid;
	//	currentpgno=0;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		currentpgno=0;
		updatepage(currentpgno);
	}

	
	public void updatepage(int currentpgno) throws TransactionAbortedException, DbException{
		HeapPageId tempHPId=new HeapPageId(heapfile.getId(),currentpgno);
	
		currentpg=(HeapPage) Database.getBufferPool().getPage(tid, tempHPId,Permissions.READ_ONLY );
		hfiterator=currentpg.iterator();
		
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(hfiterator==null)
			return false;
		
		 if(hfiterator.hasNext())
			 return true;
		 
		 if(currentpgno<heapfile.numPages()-1)
		 {
			 try {
				return judgeTuples(currentpgno+1).size()>0;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		 
		 return false;
		
		
	}
	
/*Judge if there is still next page.
We do this by scanning the tuples of next page, and see if the size of it is greater than 0. 
It sounds a little bit stupid, but I don't come up with a better way.*/
	
	private ArrayList<Tuple> judgeTuples(int currentpgno) throws Exception {
		ArrayList<Tuple> res=new ArrayList<Tuple>();
	
		try{
			PageId pid = new HeapPageId(heapfile.getId(),currentpgno);
			HeapPage temppage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
			Iterator <Tuple> tuples = temppage.iterator();
			while(tuples.hasNext()){
				res.add(tuples.next());
			}
			return res;
		} catch(Exception e){
			throw e;
		}
	}
	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		// TODO Auto-generated method stub
	
		if(hfiterator==null)
			throw new NoSuchElementException();
		
		if(!hfiterator.hasNext()&&currentpgno<heapfile.numPages()-1)
		{
			currentpgno++;
			updatepage(currentpgno);
		}
		if(hfiterator.hasNext()){
		return hfiterator.next();}
		
		return null;
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		currentpgno=0;
		//close();
		open();
	    
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
		hfiterator=null;
		
		currentpgno=0;
		heapfile=null;
	}
	
}

