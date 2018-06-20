package com.migu.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 服务节点
 *
 * @author sunyuanpeng
 * @version C10 2018年6月20日
 * @since SDP V300R003C10
 */
public class Node
{
    private int nodeId;
    
    private List<Integer> taskIdList = new ArrayList<Integer>();
    
    private List<Integer> taskCostList = new ArrayList<Integer>();
    
    private Map<Integer, Integer> taskMap = new HashMap<Integer, Integer>();
    
    /**
     * 构造函数
     */
    public Node()
    {
        super();
    }
    
    /**
     * 构造函数
     * 
     * @param nodeId
     * @param taskIdList
     */
    public Node(int nodeId)
    {
        super();
        this.nodeId = nodeId;
    }
    
    /**
     * 取得nodeId
     * 
     * @return 返回nodeId。
     */
    public int getNodeId()
    {
        return nodeId;
    }
    
    /**
     * 取得taskIdList
     * 
     * @return 返回taskIdList。
     */
    public List<Integer> getTaskIdList()
    {
        return taskIdList;
    }
    
    /**
     * 取得taskCostList
     * 
     * @return 返回taskCostList。
     */
    public List<Integer> getTaskCostList()
    {
        return taskCostList;
    }
    
    /**
     * 取得taskMap
     * 
     * @return 返回taskMap。
     */
    public Map<Integer, Integer> getTaskMap()
    {
        return taskMap;
    }
    
    /**
     * 设置nodeId
     * 
     * @param nodeId 要设置的nodeId。
     */
    public void setNodeId(int nodeId)
    {
        this.nodeId = nodeId;
    }
    
    /**
     * 设置taskIdList
     * 
     * @param taskIdList 要设置的taskIdList。
     */
    public void setTaskIdList(List<Integer> taskIdList)
    {
        this.taskIdList = taskIdList;
    }
    
    /**
     * 设置taskCostList
     * 
     * @param taskCostList 要设置的taskCostList。
     */
    public void setTaskCostList(List<Integer> taskCostList)
    {
        this.taskCostList = taskCostList;
    }
    
    /**
     * 设置taskMap
     * 
     * @param taskMap 要设置的taskMap。
     */
    public void setTaskMap(Map<Integer, Integer> taskMap)
    {
        this.taskMap = taskMap;
    }
    
    public void addTask(int taskId, int consumption)
    {
        taskIdList.add(taskId);
        taskCostList.add(consumption);
        taskMap.put(taskId, consumption);
    }
    
    public void removeTask(int taskId)
    {
        taskIdList.remove(new Integer(taskId));
        taskCostList.remove(taskMap.get(taskId));
        taskMap.remove(taskId);
    }
    
    public void clear()
    {
        taskIdList.clear();
        taskCostList.clear();
        taskMap.clear();
    }
    
    /**
     * 获取总消耗
     * 
     * @author sunyuanpeng
     * @return
     */
    public int getSumCost()
    {
        int sum = 0;
        for (int cost : taskCostList)
        {
            sum += cost;
        }
        return sum;
    }
    
    public boolean isEmpty()
    {
        return taskIdList.size() > 0;
    }
}
