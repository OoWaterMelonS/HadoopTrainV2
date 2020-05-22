package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author EaApple
 * @5/22/2020 8:24 AM
 * description：自定义复杂类型
 *
 * 1  实现writable接口
 * 2 实现接口的方法
 * 3 定义一个默认的构造方法  无参数的
 */
public class Access implements Writable {
    private String phone;
    private long up;
    private long down;
    private long sum;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return  phone+
                "," + up +
                "," + down +
                "," + sum;
    }

    public Access() {
    }

    public Access(String phone, long up, long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = this.up+this.down;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up=in.readLong();
        this.down=in.readLong();
        this.sum=in.readLong();
    }
}
