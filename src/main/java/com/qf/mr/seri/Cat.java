package com.qf.mr.seri;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  定义一个hadoop序列化的类,要实现Writable接口
 *  因为在mr中的key必须要可以排序,如果我们用自定义的对象做key
 *  那么这个对象一定要实现WritableComparable接口,并且实现其中的方法
 */
public class Cat implements WritableComparable<Cat> {
    private String name;
    private int age;
    private int price;
    private String number;

    public Cat(){

    }

    /**
     * sss 打印自定义文本数据强烈建议使用\t来进行分隔,因为文本中会有空格 , "" 等,但是\t很少,出现频率低,所以使用\t
     * @return
     */
    @Override
    public String toString() {
        return name+"\t"+age+"\t"+price+"\t"+number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cat cat = (Cat) o;

        if (age != cat.age) return false;
        if (price != cat.price) return false;
        if (name != null ? !name.equals(cat.name) : cat.name != null) return false;
        return number != null ? number.equals(cat.number) : cat.number == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + age;
        result = 31 * result + price;
        result = 31 * result + (number != null ? number.hashCode() : 0);
        return result;
    }


    /**
     * qqq write是用来持久化中写数据,注意要用传入的参数对象out进行持久化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        //如果字段类型是String,那么要调用的方法是writeUTF
        out.writeUTF(name);
        out.writeInt(age);
        out.writeInt(price);
        out.writeUTF(number);
    }

    /**
     * readFields用来做序列化中读取对象的值,使用传参对象in来进行读取,并且读取后要给当前字段赋值
     * 特别注意: 字段的读写顺序必须一致: 在write方法中写的顺序和readFields读的顺序必须一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        age = in.readInt();
        price = in.readInt();
        number = in.readUTF();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }


    @Override
    public int compareTo(Cat o) {
        int compare = Integer.compare(age, o.getAge());
        return name.compareTo(o.getName()) != 0 ? name.compareTo(o.getName()) : (compare == 0 ? 1 : compare);
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }
}
