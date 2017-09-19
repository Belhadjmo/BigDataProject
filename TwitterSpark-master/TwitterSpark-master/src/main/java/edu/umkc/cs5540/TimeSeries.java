package edu.umkc.cs5540;


import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TimeSeries implements Serializable {
    private String name;
    private List<Tuple2<String, Long>> data;

    private Long count1 = 0l;
    private Long count2 = 0l;

    TimeSeries(String name, List timestamps, List tweets) {
        this.name = name;
        List<Tuple2<String, Long>> newData = new ArrayList<>();
        if (timestamps != null && tweets != null) {
            for (int i = 0; i < timestamps.size(); i++) {
                if (tweets.get(i) != null) {
                    newData.add(new Tuple2<>(String.valueOf(timestamps.get(i)), (Long)tweets.get(i)));
                }
            }
        }
        this.data = newData;
    }

    TimeSeries(String name, Long count1, Long count2) {
        this.name = name;
        this.count1 = count1;
        this.count2 = count2;
    }

    public Long getCount1() {
        return count1;
    }

    public Long getCount2() {
        return count2;
    }

    public void setCount1(Long count) {
        this.count1 = count;
    }

    public void setCount2(Long count) {
        this.count2 = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Tuple2<String, Long>> getData() {
        return data;
    }

    public void setData(List<Tuple2<String, Long>> data) {
        this.data = data;
    }
}
