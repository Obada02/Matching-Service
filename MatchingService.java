package com.gradProj.PSUTBlender.service;

import com.gradProj.PSUTBlender.model.Supervisor;
import com.gradProj.PSUTBlender.model.SupervisorInstance;
import com.gradProj.PSUTBlender.repository.GroupsRepository;
import com.gradProj.PSUTBlender.repository.SupervisorRepository;
import com.gradProj.PSUTBlender.DTO.UserDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Math.min;

@Service
public class MatchingService {
    private static final Logger logger = LoggerFactory.getLogger(MatchingService.class);
    @Autowired
    private GroupsRepository groupsRepository;
    @Autowired
    private SupervisorRepository supervisorRepository;
    @Autowired
    private GroupRanksSupervisorService groupRanksSupervisorService;
    @Autowired
    private SupervisorRanksGroupService supervisorRanksGroupService;
    @Autowired
    private GroupsService groupsService;

    @Autowired
    private UserInSectionService userInSectionService;
    private List<Long> groupIDs = new ArrayList<>();
    private List<Long> supervisorIDs = new ArrayList<>();
    private Map<Long, List<Long>> groupsPreferenceList = new HashMap<>();
    private Map<Long, List<Long>> supervisorsPreferenceList = new HashMap<>();
    private Map<SupervisorInstance,List<Long>> supervisorInstancesPreferenceList = new HashMap<>();
    private Map<SupervisorInstance, Integer> index = new HashMap<>();
    private List<Pair<Long, Long>> results = new ArrayList<>();
    private Map<Long, SupervisorInstance> partner = new HashMap<>();
    private Map<Long, Integer> maxNumOfGroups = new HashMap<>();
    private Map<Long, Integer> minNumOfGroups = new HashMap<>();
    private Map<Long, Integer> instancesCount = new HashMap<>();
    private Queue<SupervisorInstance> supervisorQueue = new ConcurrentLinkedQueue<>();
    private Map<Long, Integer> supervisorPopularity = new HashMap<>();
    private Map<Long, Integer> groupPopularity = new HashMap<>();


    private void clearData(){
        groupIDs.clear();
        supervisorIDs.clear();
        groupsPreferenceList.clear();
        supervisorsPreferenceList.clear();
        supervisorInstancesPreferenceList.clear();
        index.clear();
        results.clear();
        partner.clear();
        maxNumOfGroups.clear();
        minNumOfGroups.clear();
        instancesCount.clear();
        supervisorQueue.clear();
        supervisorPopularity.clear();
        groupPopularity.clear();
    }

    private void fillData(){
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        UserDto userDetails = (UserDto) auth.getPrincipal();
        Long sectionID = userDetails.getSectionID();

        logger.info("Filling data for matching process");
        groupIDs = groupsRepository.findAllGroupIds(sectionID);
        //List<Supervisor> supervisors = supervisorRepository.findAllBySection_SectionID(sectionID);
        List<Supervisor> supervisors = userInSectionService.findSupervisorsBySectionid(sectionID);
        for (Supervisor supervisor : supervisors){
            Long ID = supervisor.getUserID();
            supervisorIDs.add(ID);
            maxNumOfGroups.put(ID,supervisor.getMaxNumOfGroups());
            minNumOfGroups.put(ID,supervisor.getMinNumOfGroups());
            supervisorPopularity.put(ID,0);
            logger.info("Supervisor {} added with min/max group constraints = {} , {}", ID,minNumOfGroups.get(ID), maxNumOfGroups.get(ID) );
        }
        System.out.print(supervisors);
        for (Long groupId : groupIDs) {
            List<Long> supervisorIds = groupRanksSupervisorService.getSortedSupervisorIdsForGroup(groupId,sectionID);
            groupsPreferenceList.put(groupId, supervisorIds);
            groupPopularity.put(groupId,0);
            logger.debug("Group {} preferences updated with size {}", groupId , supervisorIds.size());
        }

        for (Long supervisorId : supervisorIDs){
            List<Long> groupIds = supervisorRanksGroupService.getSortedGroupIdsForSupervisor(supervisorId , sectionID);
            supervisorsPreferenceList.put(supervisorId,groupIds);
            logger.debug("supervisor {} preferences updated with size {}", supervisorId , groupIds.size());
        }
        logger.info("Data filling complete with {} groups and {} supervisors", groupIDs.size(), supervisorIDs.size());
    }
    private void calculateSupervisorsPopularity(){
        for (Long id : groupIDs){
            List<Long> list = groupsPreferenceList.get(id);
            for (Integer i = 0; i< list.size(); i++)
                supervisorPopularity.replace(list.get(i), supervisorPopularity.get(list.get(i)) +i);
        }

        for (Long Id : supervisorIDs)
            logger.info("supervisor {} has popularity = {}", Id , supervisorPopularity.get(Id));
    }
    private void calculateGroupsPopularity(){
        for (Long id : supervisorIDs){
            List<Long> list = supervisorsPreferenceList.get(id);
            for (Integer i = 0; i<list.size();i++)
                groupPopularity.replace(list.get(i),groupPopularity.get(list.get(i)) + i);
        }

        for (Long Id : groupIDs)
            logger.info("group {} has popularity = {}",Id,groupPopularity.get(Id));
    }
    private void fillMissingData(){
        List<Long> defaultSupervisorPreferenceList = groupIDs;
        defaultSupervisorPreferenceList.sort(Comparator.comparingInt(groupPopularity::get));
        Collections.reverse(defaultSupervisorPreferenceList);

        for (Long supervisorId : supervisorIDs){
            if (!supervisorsPreferenceList.get(supervisorId).isEmpty())
                continue;
            logger.info("supervisor {} doesn't have a list",supervisorId);
            supervisorsPreferenceList.put(supervisorId,defaultSupervisorPreferenceList);
            logger.info("and now it has this {}",defaultSupervisorPreferenceList);
        }

        List<Long> defaultGroupPreferenceList = supervisorIDs;
        defaultGroupPreferenceList.sort(Comparator.comparingInt(supervisorPopularity::get));
        Collections.reverse(defaultGroupPreferenceList);

        for (Long groupId: groupIDs){
            if (!groupsPreferenceList.get(groupId).isEmpty())
                continue;
            groupsPreferenceList.put(groupId,defaultGroupPreferenceList);
        }
    }
    private void prepareData(){
        supervisorIDs.sort(Comparator.comparingInt(supervisorPopularity::get));
        System.out.print(supervisorIDs);
        System.out.println(supervisorIDs.size());

        int cnt = 0;
        for (Long id : supervisorIDs) {
            System.out.println(id);
            Integer minValue = 0;
            logger.info("minNum map {}",minNumOfGroups.get(id));
            if (minNumOfGroups.get(id) != null)
                minValue = minNumOfGroups.get(id);
            logger.info("id : {} has min value = {}",id,minValue);
            cnt += minValue;
            instancesCount.put(id,minValue);
            for (int j = 0; j < minValue; j++)
                supervisorQueue.add(new SupervisorInstance(id, j));
            logger.info("supervisor {} has min value {} , and the count is currently is {}",id , minValue,cnt);
        }

        logger.info("count after the loop is {}",cnt);
        while (cnt < groupIDs.size()){
            for (Long id : supervisorIDs) {
                Integer maxValue = 0;
                if (maxNumOfGroups.get(id) != null)
                    maxValue = maxNumOfGroups.get(id);
                if (instancesCount.get(id) < maxValue && cnt < groupIDs.size()) {
                    cnt++;
                    supervisorQueue.add(new SupervisorInstance(id, instancesCount.get(id) + 1));
                    instancesCount.replace(id,instancesCount.get(id)+1);
                }
            }
        }
        for (Long id : supervisorIDs) {
            logger.info("supervisor {} has {} instances",id , instancesCount.get(id));
        }

        for (SupervisorInstance supervisor : supervisorQueue){
            index.put(supervisor,0);
            supervisorInstancesPreferenceList.put(supervisor,
                    supervisorsPreferenceList.get(supervisor.ID));
            logger.info("/// {}",supervisorsPreferenceList.get(supervisor.ID));
            logger.info("instance {}",supervisor);
            logger.info("has {}",supervisorInstancesPreferenceList.get(supervisor));
        }
    }
    private void propose (SupervisorInstance supervisor , Long group){
        logger.info("Proposing supervisor {} for group {}", supervisor.ID, group);
        //supervisorQueue.remove(supervisor);
        if (!partner.containsKey(group)){
            partner.put(group,supervisor);
            logger.info("Supervisor {} assigned to group {} as no prior partner", supervisor.ID, group);
            return;
        }

        Integer proposerRank = groupsPreferenceList.get(group).indexOf(supervisor.ID);
        SupervisorInstance currentPartner = partner.get(group);
        int partnerRank = groupsPreferenceList.get(group).indexOf(currentPartner.ID);

        if (proposerRank < partnerRank){
            index.replace(currentPartner , index.get(currentPartner) + 1);
            partner.replace(group, currentPartner , supervisor);
            supervisorQueue.offer(currentPartner);
            logger.info("Supervisor {} replaced {} for group {} after comparison", supervisor.ID, currentPartner.ID, group);
        }else{
            index.replace(supervisor , index.get(supervisor) + 1);
            supervisorQueue.offer(supervisor);
            logger.info("Supervisor {} remains for group {} as current partner is preferred", supervisor.ID, group);
        }
    }
    public List<Pair<Long,Long>> match(){
        logger.info("Starting match process");
        clearData();
        fillData();
        calculateSupervisorsPopularity();
        calculateGroupsPopularity();
        fillMissingData();
        prepareData();

        while (!supervisorQueue.isEmpty()) {
            SupervisorInstance supervisor = supervisorQueue.poll();
            if (supervisor != null) {
                Integer idx = index.get(supervisor);
                System.out.println("Size of supervisor Instances Preference List ");
                logger.info("supervisor {} : {}",supervisor.ID, supervisorInstancesPreferenceList.get(supervisor).size());
                if (idx == null || idx >= supervisorInstancesPreferenceList.get(supervisor).size())
                    idx = supervisorInstancesPreferenceList.get(supervisor).size()-1;
                propose(supervisor, supervisorInstancesPreferenceList.get(supervisor).get(idx));
            }
        }
        storeResults();
        logger.info("Match process completed");
        return results;
    }

    public void storeResults(){
        logger.info("Storing results of match process");
        for (Long groupId : groupIDs) {
            groupsService.updateGroupSupervisor(groupId, partner.get(groupId).ID);

            results.add(Pair.of(groupId, partner.get(groupId).ID));
            logger.debug("Result stored for group {}: {}", groupId, partner.get(groupId).ID);
        }
    }
}
