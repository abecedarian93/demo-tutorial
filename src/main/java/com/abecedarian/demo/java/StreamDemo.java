package com.abecedarian.demo.java;

import javafx.util.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Created by abecedarian on 2018/11/16
 */
public class StreamDemo {

    public static void main(String[] args) {

//        streamDemo();
//        sortDemo();
//        foreachDemo();
//        filterDemo();
//        intermediateDemo();
//        terminalDemo();

    }

    /**
     * stream demo
     **/
    public static void streamDemo() {
        List<Pair<String, Integer>> lists = Arrays.asList(new Pair<>("word", 2), new Pair<>("hello", 3), new Pair<>("java", 6));

        int sum = lists.stream().filter(e -> !e.getKey().equals("hello")).mapToInt(Pair::getValue).sum();
        System.out.println(sum);
    }

    /**
     * 排序
     **/
    public static void sortDemo() {
        //常用方式
        List<Integer> list = Arrays.asList(1, 2, 6, 5, 10, 8);
        Collections.sort(list, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
        System.out.println(list);

        //stream方式
        List<Integer> list2 = Arrays.asList(1, 2, 6, 5, 10, 8);
        Object[] newList = list2.parallelStream().sorted(Comparator.comparing(Integer::intValue)).toArray();
        System.out.println(newList);
    }

    /**
     * 遍历
     **/
    public static void foreachDemo() {
        String line = "hello\tword\tstream\tjava";
        String[] items = line.split("\t");

        //常用方式
        for (String e : items) {
            System.out.println(e + "*end");
        }

        //stream方式
        Stream.of(items).forEach(e -> System.out.println(e + "*end"));
    }

    /**
     * 过滤
     **/
    public static void filterDemo() {
        List<String> list = Arrays.asList("one", "two", "three", "four", "five");

        //常用方式
        for (String e : list) {
            if ("two".equals(e)) {
                continue;
            }
            System.out.println(e + "*end");
        }

        //stream方式
        list.stream().filter(e -> !e.equals("two")).forEach(e -> System.out.println(e + "*end"));  //串行
        list.parallelStream().filter(e -> !e.equals("two")).forEach(e -> System.out.println(e + "*end")); //并行
    }


    /**
     * Intermediate
     **/
    public static void intermediateDemo() {
        List<Pair<String, Integer>> lists = Arrays.asList(new Pair<>("hello word", 2), new Pair<>("hello java", 3), new Pair<>("java stream", 6));

        //map （1对1的映射）
        lists.stream().map(line -> Arrays.stream(line.getKey().split(" "))).forEach(line -> line.forEach(e -> System.out.println(e))); //Stream<String[]>
        //mapToInt （1对1的映射）
        lists.stream().mapToInt(Pair::getValue).forEach(System.out::println);
        //flatMap （多对1的映射）
        lists.stream().flatMap(line -> Arrays.stream(line.getKey().split(" "))).forEach(System.out::println); //Stream<String>
        //filter
        lists.stream().filter(line -> line.getKey().equals("hello java")).forEach(System.out::println);
        //distinct
        lists.stream().flatMap(line -> Arrays.stream(line.getKey().split(" "))).distinct().forEach(System.out::println);
        //sorted
        lists.stream().sorted(Comparator.comparing(Pair::getValue)).forEach(System.out::println);
        //peek  (生成一个包含原Stream的所有元素的新Stream，新Stream每个元素被消费之前都会执行peek给定的消费函数)
        lists.stream().peek(line -> System.out.println(line.getKey() + "*end")).forEach(System.out::println);
        //limit
        lists.stream().limit(2).forEach(System.out::println);
        //skip
        lists.stream().skip(2).forEach(System.out::println);
        System.out.println("dd");
        //parallel
        lists.stream().parallel().forEach(System.out::println);
        lists.parallelStream().forEach(System.out::println);
        //sequential
        lists.parallelStream().sequential().forEach(System.out::println);
        lists.stream().forEach(System.out::println);
        //unordered (消除流中必须保持的有序约束，因此允许之后的操作使用不必考虑有序的优化)
        lists.stream().unordered().forEach(System.out::println);
    }

    /**
     * Terminal
     **/
    public static void terminalDemo() {
        List<Pair<String, Integer>> lists = Arrays.asList(new Pair<>("hello word", 3), new Pair<>("hello java", 3), new Pair<>("java stream", 6), new Pair<>("java hello", 5));

        //forEach
        lists.stream().forEach(System.out::println);
        //forEachOrdered
        lists.stream().forEachOrdered(System.out::println);
        //toArray
        Object[] newList = lists.stream().toArray();
        //reduce
        Pair<String, Integer> reduce = lists.stream().reduce(new Pair<>("", 0), (o1, o2) -> new Pair<>((o1.getKey() + " " + o2.getKey()), (o1.getValue() + o2.getValue())));
        System.out.println(reduce.getKey() + "\t" + reduce.getValue());
        //collect
        lists.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        Map<Integer, List<Pair<String, Integer>>> collect = lists.stream().collect(Collectors.groupingBy(Pair::getValue));
        Map<Boolean, List<Pair<String, Integer>>> collect1 = lists.stream().collect(Collectors.partitioningBy(line -> line.getValue() < 6));
        //min
        int min = lists.stream().mapToInt(Pair::getValue).reduce(Integer.MAX_VALUE, Integer::min);
        //max
        int max = lists.stream().mapToInt(Pair::getValue).reduce(Integer.MIN_VALUE, Integer::max);
        //sum
        int sum = lists.stream().mapToInt(Pair::getValue).reduce(0, Integer::sum);
        int sum1 = lists.stream().mapToInt(Pair::getValue).sum();
        //count
        long count = lists.stream().count();
        //anyMatch
        boolean anyMatchFlag = lists.stream().anyMatch(line -> line.getValue() == 3);
        //allMatch
        boolean allMatchFlag = lists.stream().allMatch(line -> line.getValue() == 3);
        //noneMatch
        boolean noneMatchFlag = lists.stream().noneMatch(line -> line.getKey().contains("hello"));
        //findFirst
        Optional<Pair<String, Integer>> pair = lists.stream().findFirst();
        //findAny
        Optional<Pair<String, Integer>> any = lists.stream().findAny();
        //iterator
        Iterator<Pair<String, Integer>> iterator = lists.stream().iterator();
        Iterator<Pair<String, Integer>> iterator1 = lists.iterator();

    }


}
