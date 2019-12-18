package com.atguigu.practice.transformer;


import com.atguigu.practice.model.Event;

import java.util.List;

public interface Transformer {
    List<Event> transform(String message);
}
