package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their associated schemas. For now, this is a stub catalog that must
 * be populated with tables by a user program before it can be used -- eventually, this should be converted to a catalog that reads a
 * catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {
    //与每个表相关联的是一个TupleDesc对象，它允许操作员确定表中字段的类型和数量

//    private ArrayList<Table> tableArrayList=new ArrayList<>();
//    private ArrayList<Integer> tableIdList = new ArrayList<>();
    public final ConcurrentHashMap<Integer,Table> hashTable;

    //Table内部类，用于组织Table
    public static class Table implements Serializable {

        public DbFile file;

        public String name;

        public String pkeyField;

        public Table(DbFile file, String name, String pkeyField) {
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }

        public String getName() {
            return this.name;
        }

        public void setName(String name) {
            this.name = name;
        }

//        public void setFile(DbFile file) {
//            this.file = file;
//        }
//
//        public void setPkeyField(String pkeyField){
//            this.pkeyField=pkeyField;
//        }
    }

    /**
     * Constructor. Creates a new, empty catalog.
     * 构造函数
     */
    public Catalog() {
        // some code goes here
        hashTable=new ConcurrentHashMap<Integer,Table>();
    }

    /**
     * Add a new table to the catalog. This table's contents are stored in the specified DbFile.
     *
     * @param file
     *         the contents of the table to add;  file.getId() is the identfier of this file/tupledesc param for the calls getTupleDesc and
     *         getFile
     * @param name
     *         the name of the table -- may be an empty string.  May not be null.  If a name conflict exists, use the last table to be added
     *         as the table for a given name.
     * @param pkeyField
     *         the name of the primary key field
     *         主键字段的名称
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        //如果数组是空的，则直接将表添加
//        if (tableArrayList.isEmpty()) {
//            tableArrayList.add(new Table(file, name, pkeyField));
//            tableIdList.add(file.getId());
//        } else {
//            for(int i=0;i<tableArrayList.size();i++){
//                //如果新加入的表和之前存储的表重名了，用新表替换旧表
//                if(tableArrayList.get(i).name.equals(name)){
//                    tableArrayList.get(i).file=file;
//                    tableIdList.set(i, file.getId());
//                }else if(tableArrayList.get(i).file.getId()==file.getId()){
//                    //如果表的id相同，则把表名和表都更新
//                    tableArrayList.get(i).name=name;
//                    tableArrayList.get(i).file=file;
//                }else {
//                    //如果不存在冲突，则直接添加
//                    tableArrayList.add(new Table(file,name,pkeyField));
//                    tableIdList.add(file.getId());
//                }
//            }
//        }
        Table t = new Table(file,name,pkeyField);
        hashTable.put(file.getId(),t);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog. This table has tuples formatted using the specified TupleDesc and its contents are stored in the
     * specified DbFile.
     *
     * @param file
     *         the contents of the table to add;  file.getId() is the identfier of this file/tupledesc param for the calls getTupleDesc and
     *         getFile
     */
    public void addTable(DbFile file) {
        //表名随机生成
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException
     *         if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if(name==null){
            throw new NoSuchElementException("输入的表名为空");
        }
        Integer res = hashTable.searchValues(1,value->{
            if(value.name.equals(name)){
                return value.file.getId();
            }
            return null;
        });
        if(res!=null){
            return res;
        }else{
            //如果表不存在，抛出异常
            throw new NoSuchElementException("不存在名为："+name+"的表");
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid
     *         The id of the table, as specified by the DbFile.getId() function passed to addTable
     * @throws NoSuchElementException
     *         if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        Table t=hashTable.getOrDefault(tableid,null);
        if(t!=null){
            return t.file.getTupleDesc();
        }else{
            throw new NoSuchElementException("不存在id为："+tableid+"的表");
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the specified table.
     * DbFile可以用来获取指定表的内容
     *
     * @param tableid
     *         The id of the table, as specified by the DbFile.getId() function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Table t=hashTable.getOrDefault(tableid,null);
        if(t!=null){
            return t.file;
        }else{
            throw new NoSuchElementException("不存在id为："+tableid+"的表");
        }
    }

    public String getPrimaryKey(int tableid) throws NoSuchElementException{
        // some code goes here
        Table t=hashTable.getOrDefault(tableid,null);
        if(t!=null){
            return t.pkeyField;
        }else{
            throw new NoSuchElementException("不存在id为："+tableid+"的表");
        }
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return hashTable.keySet().iterator();
    }

    public String getTableName(int id) throws NoSuchElementException{
        // some code goes here
        Table t=hashTable.getOrDefault(id,null);
        if(t!=null){
            return t.name;
        }else{
            throw new NoSuchElementException("不存在id为："+id+"的表");
        }
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        hashTable.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * 从文件中读取模式并在数据库中创建适当的表
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

