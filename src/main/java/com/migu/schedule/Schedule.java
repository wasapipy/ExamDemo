package com.migu.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

/*
*类名和方法不能修改
 */
public class Schedule
{
    /**
     * 服务节点id列表
     */
    private static List<Integer> nodeIdList;
    
    /**
     * 服务节点列表
     */
    private static Map<Integer, Node> nodeList;
    
    /**
     * 服务节点id列表
     */
    private static List<Integer> taskIdList;
    
    /**
     * 挂起队列
     */
    private static Map<Integer, Integer> taskList;
    
    // /**
    // * 当前任务队列
    // */
    // private static List<TaskInfo> taskInfoList;
    
    /**
     * 服务节点id是否需要排序
     */
    // private static boolean needSort = false;
    
    /**
     * 初始化
     *
     * @author sunyuanpeng
     * @return
     */
    public int init()
    {
        nodeIdList = new ArrayList<Integer>();
        nodeList = new HashMap<Integer, Node>();
        taskIdList = new ArrayList<Integer>();
        taskList = new HashMap<Integer, Integer>();
        return ReturnCodeKeys.E001;
    }
    
    /**
     * 注册节点
     *
     * @author sunyuanpeng
     * @param nodeId
     * @return
     */
    public int registerNode(int nodeId)
    {
        if (nodeId <= 0)
            return ReturnCodeKeys.E004;
        if (nodeIdList.contains(nodeId))
            return ReturnCodeKeys.E005;
        nodeIdList.add(nodeId);
        nodeList.put(nodeId, new Node(nodeId));
        // needSort = true;
        return ReturnCodeKeys.E003;
    }
    
    /**
     * 注销节点
     *
     * @author sunyuanpeng
     * @param nodeId
     * @return
     */
    public int unregisterNode(int nodeId)
    {
        if (nodeId <= 0)
            return ReturnCodeKeys.E004;
        if (!nodeIdList.contains(nodeId))
            return ReturnCodeKeys.E007;
        Node node = nodeList.get(nodeId);
        // 移除节点
        nodeList.remove(nodeId);
        // 释放任务:返回挂起队列
        if (!node.isEmpty())
        {
            Map<Integer, Integer> taskMap = node.getTaskMap();
            for (int taskId : node.getTaskIdList())
            {
                addTask(taskId, taskMap.get(taskId));
            }
        }
        return ReturnCodeKeys.E006;
    }
    
    /**
     * 添加任务
     * 
     *
     * @author sunyuanpeng
     * @param taskId
     * @param consumption
     * @return
     */
    public int addTask(int taskId, int consumption)
    {
        if (taskId <= 0)
            return ReturnCodeKeys.E009;
        if (taskList.containsKey(taskId))
            return ReturnCodeKeys.E010;
        taskIdList.add(taskId);
        taskList.put(taskId, consumption);
        return ReturnCodeKeys.E008;
    }
    
    /**
     * 删除任务
     * 
     *
     * @author sunyuanpeng
     * @param taskId
     * @return
     */
    public int deleteTask(int taskId)
    {
        if (taskId <= 0)
            return ReturnCodeKeys.E009;
        // 移除挂起队列中的任务
        if (taskList.containsKey(taskId))
        {
            taskIdList.remove(new Integer(taskId));
            taskList.remove(taskId);
            return ReturnCodeKeys.E011;
        }
        // 移除服务节点上的任务
        for (int nodeId : nodeIdList)
        {
            Node node = nodeList.get(nodeId);
            if (node.getTaskIdList().contains(taskId))
            {
                node.removeTask(taskId);
                return ReturnCodeKeys.E011;
            }
        }
        return ReturnCodeKeys.E012;
    }
    
    /**
     * 任务调度
     * 
     *
     * @author sunyuanpeng
     * @param threshold
     * @return
     */
    public int scheduleTask(int threshold)
    {
        if (threshold <= 0)
        {
            return ReturnCodeKeys.E002;
        }
        
        // 挂起任务分配
        if (!taskIdList.isEmpty())
        {
            int avg = getAVG();
            
            int cost = getMaxCostTask(0);
            while (cost != 0)
            {
                boolean flag = true;
                for (int nodeId : nodeIdList)
                {
                    Node node = nodeList.get(nodeId);
                    if (node.getSumCost() + cost <= avg)
                    {
                        int taskId = getKey(cost);
                        node.addTask(taskId, cost);
                        taskIdList.remove(new Integer(taskId));
                        taskList.remove(taskId);
                        flag = false;
                        break;
                    }
                }
                if (flag)
                {
                    return ReturnCodeKeys.E014;
                }
                cost = getMaxCostTask(cost);
            }
            // 处理顺序
            exchange();
            // print();
            return ReturnCodeKeys.E013;
        }
        
        if (exchange())
            return ReturnCodeKeys.E013;
        return ReturnCodeKeys.E014;
    }
    
    /**
     * 查询任务状态
     * 
     *
     * @author sunyuanpeng
     * @param tasks
     * @return
     */
    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        if (tasks == null)
            return ReturnCodeKeys.E016;
        
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        List<Integer> taskIdList = new ArrayList<Integer>();
        for (int nodeId : nodeIdList)
        {
            Node node = nodeList.get(nodeId);
            int nid = node.getNodeId();
            for (int taskId : node.getTaskIdList())
            {
                map.put(taskId, nid);
                taskIdList.add(taskId);
            }
        }
        
        taskIdList = sort(taskIdList);
        for (int taskId : taskIdList)
        {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setTaskId(taskId);
            taskInfo.setNodeId(map.get(taskId));
            tasks.add(taskInfo);
        }
        return ReturnCodeKeys.E015;
    }
    
    private static void print()
    {
        for (int nodeId : nodeIdList)
        {
            Node node = nodeList.get(nodeId);
            System.out.println(node.getNodeId() + ":" + node.getTaskMap());
        }
    }
    
    /**
     * 任务重整: 1.消耗率大的编号大 2.消耗率相同时 编号小的任务少
     *
     * @author sunyuanpeng
     */
    private static boolean exchange()
    {
        boolean flag = false;
        nodeIdList = sort(nodeIdList);
        // 将消耗率小的换给编号小的
        for (int i = 0; i < nodeIdList.size(); i++)
        {
            Node nodeI = nodeList.get(nodeIdList.get(i));
            int sumI = nodeI.getSumCost();
            for (int j = i + 1; j < nodeIdList.size(); j++)
            {
                Node nodeJ = nodeList.get(nodeIdList.get(j));
                int sumJ = nodeJ.getSumCost();
                if (sumI > sumJ)
                {
                    flag = true;
                    int nodeId = nodeI.getNodeId();
                    nodeI.setNodeId(nodeJ.getNodeId());
                    nodeJ.setNodeId(nodeId);
                    Node tempNode = nodeI;
                    nodeList.put(nodeIdList.get(i), nodeJ);
                    nodeList.put(nodeIdList.get(j), tempNode);
                }
            }
        }
        
        // 消耗率相同时 编号小的任务少
        for (int i = 0; i < nodeIdList.size(); i++)
        {
            Node nodeI = nodeList.get(nodeIdList.get(i));
            int sumI = nodeI.getSumCost();
            int indexI = nodeI.getTaskIdList().size();
            for (int j = i + 1; j < nodeIdList.size(); j++)
            {
                Node nodeJ = nodeList.get(nodeIdList.get(j));
                int sumJ = nodeJ.getSumCost();
                int indexJ = nodeJ.getTaskIdList().size();
                if (sumI == sumJ && indexI > indexJ)
                {
                    flag = true;
                    int nodeId = nodeI.getNodeId();
                    nodeI.setNodeId(nodeJ.getNodeId());
                    nodeJ.setNodeId(nodeId);
                    Node tempNode = nodeI;
                    nodeList.put(nodeIdList.get(i), nodeJ);
                    nodeList.put(nodeIdList.get(j), tempNode);
                }
            }
        }
        return flag;
    }
    
    private static int getMaxCostTask(int cost)
    {
        int res = 0;
        if (cost == 0)
            cost = Integer.MAX_VALUE;
        for (int i : taskIdList)
        {
            int _cost = taskList.get(i);
            if (_cost > res && _cost <= cost)
                res = _cost;
        }
        return res;
    }
    
    /**
     * 获取任务总处理时间的平均值
     *
     * @author sunyuanpeng
     * @return
     */
    private static int getAVG()
    {
        double sum = 0;
        for (int taskId : taskIdList)
        {
            sum += taskList.get(taskId);
        }
        
        return (int)Math.ceil(sum / nodeIdList.size());
    }
    
    @SuppressWarnings("rawtypes")
    private static int getKey(int value)
    {
        Set set = taskList.entrySet();
        Iterator it = set.iterator();
        while (it.hasNext())
        {
            Map.Entry entry = (Map.Entry)it.next();
            if (entry.getValue().equals(value))
            {
                return (Integer)entry.getKey();
            }
        }
        return -1;
    }
    
    /**
     * 服务节点列表中最大和最小的差值
     *
     * @author sunyuanpeng
     * @param node1
     * @param node2
     * @return
     */
    // private int getDiff()
    // {
    // return node1.getSum() - node2.getSum();
    // }
    
    /**
     * 返回不小于nodeId的最大节点id,若nodeId为空,则返回最大节点id
     *
     * @author sunyuanpeng
     * @param nodeId
     * @return
     */
    // private int getMaxNodeId(int nodeId)
    // {
    // int res = 0;
    // if (nodeId == 0)
    // nodeId = Integer.MAX_VALUE;
    // for (int i : nodeIdList)
    // {
    // if (i > res && i < nodeId)
    // res = i;
    // }
    // if (res == 0)
    // return getMaxNodeId(0);
    // return res;
    // }
    
    /**
     * 循环获取下一个节点id
     * 
     * @author sunyuanpeng
     * @param nodeId
     * @return
     */
    private int getNextNode(int nodeId)
    {
        int index = nodeIdList.indexOf(nodeId);
        if (index == nodeIdList.size())
        {
            return nodeIdList.get(0);
        }
        return nodeIdList.get(index + 1);
    }
    
    /**
     * 快速排序:升序
     * 
     *
     * @author sunyuanpeng
     * @param list
     * @return
     */
    private static List<Integer> sort(List<Integer> list)
    {
        if (list.size() == 0)
            return list;
        Integer pivot = list.get(0);
        List<Integer> lList = new ArrayList<Integer>();
        List<Integer> rList = new ArrayList<Integer>();
        for (Integer e : list)
        {
            if (e < pivot)
            {
                lList.add(e);
            }
            if (e > pivot)
            {
                rList.add(e);
            }
        }
        List<Integer> sorted = new ArrayList<Integer>();
        sorted.addAll(sort(lList));
        sorted.add(pivot);
        sorted.addAll(sort(rList));
        return sorted;
    }
    
    public static void main(String[] args)
    {
        Schedule schedule = new Schedule();
        schedule.init();
        schedule.registerNode(1);
        schedule.registerNode(3);
        
        schedule.addTask(1, 30);
        schedule.addTask(2, 30);
        schedule.addTask(3, 30);
        schedule.addTask(4, 30);
        
        schedule.scheduleTask(10);
        
        List<TaskInfo> tasks = new ArrayList<TaskInfo>();
        schedule.queryTaskStatus(tasks);
        System.out.println(tasks);
    }
}
