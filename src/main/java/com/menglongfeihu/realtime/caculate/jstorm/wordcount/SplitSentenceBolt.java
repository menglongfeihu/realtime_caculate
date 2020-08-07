package com.menglongfeihu.realtime.caculate.jstorm.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @author lianzeng
 * @version 1.0
 * @date 2020/8/7 : 2:09 下午
 * @description
 */
public class SplitSentenceBolt extends BaseRichBolt {
    //BaseRichBolt是IComponent和IBolt接口的实现
    //继承这个类，就不用去实现本例不关心的方法

    private OutputCollector collector;

    /**
     * prepare()方法类似于ISpout 的open()方法。
     * 这个方法在blot初始化时调用，可以用来准备bolt用到的资源,比如数据库连接。
     * 本例子和SentenceSpout类一样,SplitSentenceBolt类不需要太多额外的初始化,
     * 所以prepare()方法只保存OutputCollector对象的引用。
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;

    }

    /**
     * SplitSentenceBolt核心功能是在类IBolt定义execute()方法，这个方法是IBolt接口中定义。
     * 每次Bolt从流接收一个订阅的tuple，都会调用这个方法。
     * 本例中,收到的元组中查找“sentence”的值,
     * 并将该值拆分成单个的词,然后按单词发出新的tuple。
     */
    @Override
    public void execute(Tuple input) {
        // TODO Auto-generated method stub
        String sentence = input.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for (String word : words) {
            this.collector.emit(new Values(word));//向下一个bolt发射数据
        }
    }

    /**
     * plitSentenceBolt类定义一个元组流,每个包含一个字段(“word”)。
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("word"));
    }

}