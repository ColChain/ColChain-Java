package org.colchain.colchain.community;

import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.util.RandomString;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Community {
    public enum MemberType {
        PARTICIPANT, OBSERVER
    }

    private final String id;
    private final String name;
    private MemberType memberType;
    private final Set<CommunityMember> participants;
    private final Set<CommunityMember> observers;
    private Set<String> fragmentIds = new HashSet<>();

    public Community(String id, String name, MemberType memberType, Set<CommunityMember> participants, Set<CommunityMember> observers) {
        this.id = id;
        this.name = name;
        this.memberType = memberType;
        this.participants = participants;
        this.observers = observers;
    }

    public Community(String id, String name, MemberType memberType, Set<CommunityMember> participants, Set<CommunityMember> observers, Set<String> fragmentIds) {
        this.id = id;
        this.name = name;
        this.memberType = memberType;
        this.participants = participants;
        this.observers = observers;
        this.fragmentIds = fragmentIds;
    }

    public Community(String name, MemberType memberType, Set<CommunityMember> participants, Set<CommunityMember> observers) {
        this.name = name;
        this.memberType = memberType;
        this.participants = participants;
        this.observers = observers;
        RandomString gen = new RandomString();
        id = gen.nextString();
    }

    public Community(String name) {
        this.name = name;
        this.memberType = MemberType.PARTICIPANT;
        this.participants = new HashSet<>();
        this.observers = new HashSet<>();
        RandomString gen = new RandomString();
        id = gen.nextString();

        CommunityMember mem = new CommunityMember(AbstractNode.getState().getId(), AbstractNode.getState().getAddress());
        participants.add(mem);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setMemberType(MemberType t) {
        this.memberType = t;
    }

    public void addParticipant(CommunityMember m)  {
        this.participants.add(m);
    }

    public boolean containsParticipant(String id) {
        for(CommunityMember m : participants) {
            if(m.getId().equals(id)) return true;
        }
        return false;
    }

    public CommunityMember getParticipant() {
        for(CommunityMember m : participants) {
            return m;
        }
        return null;
    }

    public void addObserver(CommunityMember m)  {
        this.observers.add(m);
    }

    public void removeMember(CommunityMember m) {
        this.participants.remove(m);
        this.observers.remove(m);
    }

    public void addFragment(String id) {
        fragmentIds.add(id);
    }

    public Set<String> getFragmentIds() {
        return fragmentIds;
    }

    public MemberType getMemberType() {
        return memberType;
    }

    public Set<CommunityMember> getParticipants() {
        return participants;
    }

    public Set<CommunityMember> getObservers() {
        return observers;
    }

    public boolean isIn(String fid) {
        return fragmentIds.contains(fid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Community community = (Community) o;
        return Objects.equals(id, community.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
