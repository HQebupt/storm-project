## 异常订购RealTimeCacheList设计说明

### `解决问题`
- 需求：规则一：用户有SessionId且对图书有订购行为，且前一小时后5分钟内对该产品的点击pv<=5。
- 解决办法：需要定义一个数据结构，能够保持此结构中的数据会定时过期 -- RealTimeCacheList。

### `设计细节`
```Java
    private Map<T, LinkedList<Long>> oldList;
    private Map<T, LinkedList<Long>> currentList;
```
- 数据结构： 数据结构为两个Map，Key为需要存储的数据。value为一个链表。存储数据到达的时间。

```Java
    protected final static Object LOCK = new Object();
    protected Thread cleaner = null;
    protected TimeOutCallback timeOutCallback = null;
    protected int expiratonSecs = 0;
```
- 由于设计到并发。所以需要对操作上述两链表的操作进行加锁。
- cleaner 为定时清理线程。expirationSecs 为超时时间，cleaner会根据expirationSecs 定期对过期数据进行清理。
- 具体的清理操作为:
```Java
cleaner = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        cleaner.sleep(sleepTime);
                        if (timeOutCallback != null) {
                            Iterator<T> iterator = oldList.keySet().iterator();
                            while (iterator.hasNext()) {
                                T key = iterator.next();
                                timeOutCallback.expire(key, oldList.get(key));
                            }
                        }
                        oldList.clear();
                        oldList.putAll(currentList);
                        currentList.clear();
                    } catch (InterruptedException e) {
                        log.error(e);
                    }
                }
            }
        });
        cleaner.setDaemon(true);
        cleaner.start();
```
	*  ① 对oldList中已经超时的数据行处理。这里交给用户自己实现。可以对其进行持久化操作。
	*  ② 对oldList进行清空。
	*  ③ 将currentList中的元素全部放入oldList中。
	*  ④ 将currentList进行清空。


- 每次插入数据的时候同样会进行清理：
```Java
private void removeExpiredData(T value, long currentTime) {
        synchronized (LOCK) {
            if (!oldList.containsKey(value)) {
                return;
            }
            long timeOutThreshold = currentTime - expiratonSecs * 1000L;
            Iterator<Long> it = oldList.get(value).iterator();
            while (it.hasNext()) {
                Long time = it.next();
                if (time < timeOutThreshold) {
                    it.remove();
                } else {
                    return;
                }
            }
        }
    }
```
	* 此操作对某个id下的过期数据进行清理。 ps:此操作似乎可以省略。

### `补充：插入数据`
```Java
public void put(T value, Long date) {
        synchronized (LOCK) {
            long currentTime;
            if (date == null) {
                currentTime = date;
            } else {
                currentTime = System.currentTimeMillis();
            }

            //clear expired data when insert data
            removeExpiredData(value, currentTime);

            if (oldList.containsKey(value)) {
                LinkedList<Long> clickTimes = oldList.get(value);
                LinkedList<Long> newClickTimes = new LinkedList<Long>();
                newClickTimes.addAll(clickTimes);
                newClickTimes.add(currentTime);
                currentList.put(value, newClickTimes);
                oldList.remove(value);
            } else {
                LinkedList<Long> clickTimes = new LinkedList<Long>();
                clickTimes.add(currentTime);
                currentList.put(value, clickTimes);
            }
        }
    }
```
- 先在oldList中寻找是否有此Key。如果有则将其从oldList移除，将新数据和老数据一起插入到currentList中。
- 如果没有此Key，则直接插入到currentList中。













