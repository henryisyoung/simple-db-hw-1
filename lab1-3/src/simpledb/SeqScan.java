package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private boolean isopen=false;
    private TransactionId tid;
    private int tableid;
    private HeapFile hpfile;
    private HeapPage hppage;
    private String tableAlias;
    private HeapFileIterator SSIterator;
    private int currentpgno;
  
    private TupleDesc td;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     * @throws TransactionAbortedException 
     * @throws DbException 
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	this.tid=tid;
    	reset(tableid,tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     * @throws TransactionAbortedException 
     * @throws DbException 
     */
    public void reset(int tableid, String tableAlias)  {
        // some code goes here
    	this.tableid=tableid;
    	this.tableAlias=tableAlias;
    	//open();
 
    	hpfile=(HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	
    	SSIterator=new HeapFileIterator(hpfile,tid);
    }

    public SeqScan(TransactionId tid, int tableid) throws DbException, TransactionAbortedException {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	isopen=true;
    	//HeapPageId pid=new HeapPageId(tableid,currentpgno);
    	//hppage=(HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    	
    	SSIterator.open();
    	this.updateTupleDesc();
    }

   /* public void updatepage() throws TransactionAbortedException, DbException
    {
    	if(currentpgno>=hpfile.numPages()-1)
    	{
    		throw new DbException("pages exceed");
    	}
   // 	HeapPageId pid=new HeapPageId(tableid,currentpgno);
    	//hpfile=(HeapFile) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    	hpfile=(HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    //	hppage=(HeapPage) hpfile.readPage(pid);
    	SSIterator=new HeapFileIterator(hpfile,tid);
    }
    */
    public void updateTupleDesc()
    {
    	int tdlength=Database.getCatalog().getTupleDesc(tableid).numFields();
    	String[] fieldAr=new String[tdlength];
    	Type[] typeAr=new Type[tdlength];
    	for(int i=0;i<tdlength;i++)
    	{
    		String fieldName=Database.getCatalog().getTupleDesc(tableid).getFieldName(i);
    		/*if(tableAlias==null||fieldName==null)
    		{
    			if(tableAlias!=null)
    		fieldAr[i]=tableAlias+"."+"null";
    			else if(fieldName!=null)
    				fieldAr[i]="null."+fieldName;
    			else
    				fieldAr[i]="null.null";
    		}*/
    		fieldAr[i]=tableAlias+"."+fieldName;
    		typeAr[i]=Database.getCatalog().getTupleDesc(tableid).getFieldType(i);
    	}
    	this.td=new TupleDesc(typeAr,fieldAr);
    	
    	
    	//SSIterator= tempfile.iterator(tid);
    }
    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    /*	if(SSIterator==null)
    		return false;*/
        return (SSIterator!=null&&SSIterator.hasNext());
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	/*if(SSIterator==null)
    		throw new NoSuchElementException("Iterator is null");
    	
    	if(!SSIterator.hasNext()&&currentpgno<hpfile.numPages()-1)
    	{
    		currentpgno++;

    		updatepage();
    	}
    	 */
    	if(SSIterator.hasNext())
    	{
    		return SSIterator.next();
    	}
    	throw new NoSuchElementException();
    }

    public void close() {
    	isopen=false;
    	if(SSIterator!=null)
    	{SSIterator.close();}
    	currentpgno=0;
    	hpfile=null;
    	hppage=null;
       
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	
    	SSIterator.rewind();
    }
}
