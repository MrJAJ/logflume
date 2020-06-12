package com.log.logflume.Entity;

import redis.clients.jedis.Jedis;

import java.io.Serializable;

public class AlarmParam implements Serializable {
    private String keyWord;
    private int errorThreshold;
    private float errorrateThreshold;

    public AlarmParam() {
    }

    public AlarmParam(String keyWord, int errorThreshold, float errorrateThreshold) {
        this.keyWord = keyWord;
        this.errorThreshold = errorThreshold;
        this.errorrateThreshold = errorrateThreshold;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public int getErrorThreshold() {
        return errorThreshold;
    }

    public void setErrorThreshold(int errorThreshold) {
        this.errorThreshold = errorThreshold;
    }

    public float getErrorrateThreshold() {
        return errorrateThreshold;
    }

    public void setErrorrateThreshold(float errorrateThreshold) {
        this.errorrateThreshold = errorrateThreshold;
    }

    public void keyWordAlarm(String message){
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        jedis.hset("Anomy","1\t"+System.currentTimeMillis(),message);
        jedis.close();
    }

    public void thresholdAlarm(String message){
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        jedis.hset("Anomy","2\t"+System.currentTimeMillis(),message);
        jedis.close();
    }

    public void errorrateThresholdAlarm(String message){
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        jedis.hset("Anomy","2\t"+System.currentTimeMillis(),message);
        jedis.close();
    }

    //drl
//package com.log.logflume.Entity
//    import com.log.logflume.Entity.AlarmParam;
//
//    rule "rule1"
//    salience 1
//    when
//    a:AlarmParam(keyWord matches ".*异常.*")
//    then
//        a.keyWordAlarm("关键字异常告警"+a.getKeyWord());
//    end
//
//    rule "rule2"
//    salience 1
//    when
//    a:AlarmParam(errorThreshold >= 10000)
//    then
//        a.thresholdAlarm("阈值异常，错误日志量高达"+a.getErrorThreshold());
//    end
//
//    rule "rule3"
//    salience 1
//    when
//    a:AlarmParam(errorrateThreshold >= 0.3)
//    then
//        a.errorrateThresholdAlarm("阈值异常，错误率高达"+a.getErrorrateThreshold());
//    end
}
