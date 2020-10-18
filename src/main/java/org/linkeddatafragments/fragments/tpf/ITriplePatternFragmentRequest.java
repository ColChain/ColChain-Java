package org.linkeddatafragments.fragments.tpf;

import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;

 
public interface ITriplePatternFragmentRequest<ConstantTermType,NamedVarType,AnonVarType>
    extends ILinkedDataFragmentRequest
{

    /**
     *
     */
    public final static String PARAMETERNAME_SUBJ = "subject";

    /**
     *
     */
    public final static String PARAMETERNAME_PRED = "predicate";

    /**
     *
     */
    public final static String PARAMETERNAME_OBJ = "object";

    /**
     * Returns the subject position of the requested triple pattern.
     * @return 
     */
    ITriplePatternElement<ConstantTermType,NamedVarType,AnonVarType> getSubject();

    /**
     * Returns the predicate position of the requested triple pattern.
     * @return 
     */
    ITriplePatternElement<ConstantTermType,NamedVarType,AnonVarType> getPredicate();

    /**
     * Returns the object position of the requested triple pattern.
     * @return 
     */
    ITriplePatternElement<ConstantTermType,NamedVarType,AnonVarType> getObject();
}
