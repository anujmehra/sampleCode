package com.am.analytics.job.service.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;

import com.am.analytics.common.core.utils.ArrayUtils;
import com.am.analytics.common.core.utils.StringUtils;
import com.am.analytics.job.common.constant.CustomerIDConstants;

/**
 * The Class CustomerIDGenerationMapsModel.
 */
public class CustomerIDHistoryModel implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The Constant SEPRATOR. */
    private static final String SEPRATOR = "/";

    /** The sequence num. */
    private long sequenceNum;

    /** The last name DOB map. */
    private Map<String, PNRSCustomerID> lastNameDOBMap;

    /** The last name doc NBR map. */
    private Map<String, PNRSCustomerID> lastNameDocNBRMap;

    /** The last name FT nbr map. */
    private Map<String, PNRSCustomerID> lastNameFTNbrMap;

    /** The last name email map. */
    private Map<String, PNRSCustomerID> lastNameEmailMap;

    /** The dob doc NBR map. */
    private Map<String, PNRSCustomerID> dobDocNBRMap;

    /** The dob FT nbr map. */
    private Map<String, PNRSCustomerID> dobFTNbrMap;

    /** The dob email map. */
    private Map<String, PNRSCustomerID> dobEmailMap;

    /** The email ID map. */
    private Map<String, PNRSCustomerID> emailIDMap;

    /** The dob map. */
    private Map<String, PNRSCustomerID> dobMap;

    /** The doc and FT number map. */
    private Map<String, PNRSCustomerID> docAndFTNumberMap;

    /** The only one field present map. */
    private Map<String, PNRSCustomerID> onlyOneFieldPresentMap;

    private Map<String, PNRSCustomerID> onlyTwoFieldPresentMap;

    /**
     * Gets the by first name.
     *
     * @param firstName the first name
     * @return the by first name
     */
    public String getByFirstName(final String firstName) {

        return StringUtils.concatenate(firstName, sequenceNum++);

    }

    /**
     * Gets the by last name DOB.
     *
     * @param lastName the last name
     * @param dob the dob
     * @return the by last name DOB
     */
    public String getByLastNameDOB(final String lastName, final String dob) {

        String customerID = null;
        if (null != lastNameDOBMap && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(dob)) {
            final String lastNameDOB = StringUtils.toLowerCase(StringUtils.concatenate(lastName, dob));

            final PNRSCustomerID value = lastNameDOBMap.get(lastNameDOB);
            customerID = (value != null ? value.getCustomerID() : null);
        }
        return customerID;
    }

    public int getCountByLastNameDOB(final String lastName, final String dob) {

        int count = 0;
        if (null != lastNameDOBMap && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(dob)) {
            final String lastNameDOB = StringUtils.toLowerCase(StringUtils.concatenate(lastName, dob));

            final PNRSCustomerID value = lastNameDOBMap.get(lastNameDOB);
            count = value != null ? value.getCount() : 0;
        }
        return count;
    }

    public void removeKeyByLastNameDOB(final String lastName, final String dob) {
        if (null != lastNameDOBMap && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(dob)) {
            final String lastNameDOB = StringUtils.toLowerCase(StringUtils.concatenate(lastName, dob));
            lastNameDOBMap.remove(lastNameDOB);
        }
    }

    /**
     * Sets the by last name DOB.
     *
     * @param lastName the last name
     * @param dob the dob
     * @param customerID the customer ID
     */
    public void setByLastNameDOB(final String lastName, final String dob, final String customerID) {

        if (!StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(dob)) {
            final String lastNameDOB = StringUtils.toLowerCase(StringUtils.concatenate(lastName, dob));
            if (null == lastNameDOBMap) {
                lastNameDOBMap = new HashMap<>();
            }
            this.updateCustomerIdMapBasedOnCount(customerID, lastNameDOB, lastNameDOBMap);
        }
    }

    public void updateCountLastNameDOBMap(final String lastName, final String dob, final int countToAdd) {

        if (!StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(dob)) {
            final String lastNameDOB = StringUtils.toLowerCase(StringUtils.concatenate(lastName, dob));
            if (null != lastNameDOBMap) {
                PNRSCustomerID pnrsCustomerID = lastNameDOBMap.get(lastNameDOB);
                if (null != pnrsCustomerID) {
                    int count = pnrsCustomerID.getCount() + countToAdd;
                    if (count > 0) {
                        pnrsCustomerID.setCount(count);
                    } else {
                        lastNameDOBMap.remove(lastNameDOB);
                    }
                }
            }
        }

    }

    /**
     * Gets the by last name doc NBR.
     *
     * @param lastName the last name
     * @param docNbr the doc nbr
     * @return the by last name doc NBR
     */
    public String getByLastNameDocNBR(final String lastName, final String docNbr) {

        String customerID = null;
        PNRSCustomerID pnrsCustomerID = null;
        if (null != lastNameDocNBRMap && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(docNbr)) {
            for (final String docNumber : docNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(docNumber)) {
                    final String lastNameDocNBR = StringUtils.toLowerCase(StringUtils.concatenate(lastName, docNumber));
                    pnrsCustomerID = lastNameDocNBRMap.get(lastNameDocNBR);
                    if (null != pnrsCustomerID) {
                        customerID = pnrsCustomerID.getCustomerID();
                        break;
                    }
                }
            }
        }
        return customerID;
    }

    /**
     * Sets the by last name doc NBR.
     *
     * @param lastName the last name
     * @param docNbr the doc nbr
     * @param customerID the customer ID
     */
    public void setByLastNameDocNBR(final String lastName, final String docNbr, final String customerID) {

        if (!StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(docNbr)) {
            if (null == lastNameDocNBRMap) {
                lastNameDocNBRMap = new HashMap<>();
            }
            for (final String docNum : docNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(docNum)) {
                    final String lastNameDocNBR = StringUtils.toLowerCase(StringUtils.concatenate(lastName, docNum));
                    this.updateCustomerIdMapBasedOnCount(customerID, lastNameDocNBR, lastNameDocNBRMap);
                }
            }
        }
    }

    /**
     * Gets the by last name ft nbr.
     *
     * @param lastName the last name
     * @param ftNbr the ft nbr
     * @return the by last name ft nbr
     */
    public String getByLastNameFtNbr(final String lastName, final String ftNbr) {

        String customerID = null;
        PNRSCustomerID pnrsCustomerID = null;
        if (null != lastNameFTNbrMap && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(ftNbr)) {
            for (final String ftNumber : ftNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(ftNumber)) {
                    final String lastNameFTNbr = StringUtils.toLowerCase(StringUtils.concatenate(lastName, ftNumber));
                    pnrsCustomerID = lastNameFTNbrMap.get(lastNameFTNbr);
                    if (null != pnrsCustomerID) {
                        customerID = pnrsCustomerID.getCustomerID();
                    }
                }
            }
        }
        return customerID;
    }

    /**
     * Sets the by last name ft nbr.
     *
     * @param lastName the last name
     * @param ftNbr the ft nbr
     * @param customerID the customer ID
     */
    public void setByLastNameFtNbr(final String lastName, final String ftNbr, final String customerID) {

        if (!StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(ftNbr)) {
            if (null == lastNameFTNbrMap) {
                lastNameFTNbrMap = new HashMap<>();
            }
            for (final String ftNum : ftNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(ftNum)) {
                    final String lastNameFTNbr = StringUtils.toLowerCase(StringUtils.concatenate(lastName, ftNum));

                    this.updateCustomerIdMapBasedOnCount(customerID, lastNameFTNbr, lastNameFTNbrMap);
                }
            }
        }
    }

    /**
     * Gets the by last name email.
     *
     * @param lastName the last name
     * @param email the email
     * @return the by last name email
     */
    public String getByLastNameEmail(final String lastName, final String email) {

        String customerID = null;
        PNRSCustomerID pnrsCustomerID = null;
        if (null != lastNameEmailMap && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(email)) {
            for (final String emailAddress : email.split(SEPRATOR)) {
                if (!StringUtils.isBlank(emailAddress)) {
                    final String lastNameEmail = StringUtils.toLowerCase(StringUtils.concatenate(lastName, emailAddress));
                    pnrsCustomerID = lastNameEmailMap.get(lastNameEmail);
                    if (null != pnrsCustomerID) {
                        customerID = pnrsCustomerID.getCustomerID();
                        break;
                    }
                }
            }
        }
        return customerID;
    }

    /**
     * Sets the by last name email.
     *
     * @param lastName the last name
     * @param email the email
     * @param customerID the customer ID
     */
    public void setByLastNameEmail(final String lastName, final String email, final String customerID) {

        if (!StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(email)) {
            if (null == lastNameEmailMap) {
                lastNameEmailMap = new HashMap<>();
            }
            for (final String emailAddress : email.split(SEPRATOR)) {
                if (!StringUtils.isBlank(emailAddress)) {
                    final String lastNameEmail = StringUtils.toLowerCase(StringUtils.concatenate(lastName, emailAddress));

                    this.updateCustomerIdMapBasedOnCount(customerID, lastNameEmail, lastNameEmailMap);
                }
            }

        }
    }

    /**
     * Gets the by DOB doc NBR.
     *
     * @param dob the dob
     * @param docNbr the doc nbr
     * @return the by DOB doc NBR
     */
    public String getByDOBDocNBR(final String dob, final String docNbr) {

        String customerID = null;
        PNRSCustomerID pnrsCustomerID = null;
        if (null != dobDocNBRMap && !StringUtils.isEmpty(dob) && !StringUtils.isEmpty(docNbr)) {
            for (final String docNumber : docNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(docNumber)) {
                    pnrsCustomerID = dobDocNBRMap.get(StringUtils.toLowerCase(StringUtils.concatenate(dob, docNumber)));
                    if (null != pnrsCustomerID) {
                        customerID = pnrsCustomerID.getCustomerID();
                        break;
                    }
                }

            }
        }
        return customerID;
    }

    /**
     * Sets the by DOB doc NBR.
     *
     * @param dob the dob
     * @param docNbr the doc nbr
     * @param customerID the customer ID
     */
    public void setByDOBDocNBR(final String dob, final String docNbr, final String customerID) {

        if (!StringUtils.isEmpty(dob) && !StringUtils.isEmpty(docNbr)) {
            if (null == dobDocNBRMap) {
                dobDocNBRMap = new HashMap<>();
            }
            for (final String docNum : docNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(docNum)) {
                    final String dobDocNBR = StringUtils.toLowerCase(StringUtils.concatenate(dob, docNum));
                    this.updateCustomerIdMapBasedOnCount(customerID, dobDocNBR, dobDocNBRMap);
                }
            }

        }
    }

    /**
     * Gets the by DOB ft nbr.
     *
     * @param dob the dob
     * @param ftNbr the ft nbr
     * @return the by DOB ft nbr
     */
    public String getByDOBFtNbr(final String dob, final String ftNbr) {

        String customerID = null;
        PNRSCustomerID pnrsCustomerID = null;
        if (null != dobFTNbrMap && !StringUtils.isEmpty(dob) && !StringUtils.isEmpty(ftNbr)) {
            for (final String ftNumber : ftNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(ftNumber)) {
                    final String dobFTNbr = StringUtils.toLowerCase(StringUtils.concatenate(dob, ftNumber));
                    pnrsCustomerID = dobFTNbrMap.get(dobFTNbr);
                    if (null != pnrsCustomerID) {
                        customerID = pnrsCustomerID.getCustomerID();
                        break;
                    }
                }
            }
        }
        return customerID;
    }

    /**
     * Sets the by DOB ft nbr.
     *
     * @param dob the dob
     * @param ftNbr the ft nbr
     * @param customerID the customer ID
     */
    public void setByDOBFtNbr(final String dob, final String ftNbr, final String customerID) {

        if (!StringUtils.isEmpty(dob) && !StringUtils.isEmpty(ftNbr)) {
            if (null == dobFTNbrMap) {
                dobFTNbrMap = new HashMap<>();
            }
            for (final String ftNum : ftNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(ftNum)) {
                    final String dobFTNbr = StringUtils.toLowerCase(StringUtils.concatenate(dob, ftNum));
                    this.updateCustomerIdMapBasedOnCount(customerID, dobFTNbr, dobFTNbrMap);
                }
            }

        }
    }

    /**
     * Gets the by DOB email.
     *
     * @param dob the dob
     * @param email the email
     * @return the by DOB email
     */
    public String getByDOBEmail(final String dob, final String email) {

        String customerID = null;
        PNRSCustomerID pnrsCustomerID = null;
        if (null != dobEmailMap && !StringUtils.isEmpty(dob) && !StringUtils.isEmpty(email)) {
            for (final String emailAddress : email.split(SEPRATOR)) {
                if (!StringUtils.isBlank(emailAddress)) {
                    final String dobEmail = StringUtils.toLowerCase(StringUtils.concatenate(dob, emailAddress));
                    pnrsCustomerID = dobEmailMap.get(dobEmail);
                    if (null != pnrsCustomerID) {
                        customerID = pnrsCustomerID.getCustomerID();
                        break;
                    }
                }
            }

        }
        return customerID;
    }

    /**
     * Sets the by DOB email.
     *
     * @param dob the dob
     * @param email the email
     * @param customerID the customer ID
     */
    public void setByDOBEmail(final String dob, final String email, final String customerID) {

        if (!StringUtils.isEmpty(dob) && !StringUtils.isEmpty(email)) {
            if (null == dobEmailMap) {
                dobEmailMap = new HashMap<>();
            }
            for (final String emailAddress : email.split(SEPRATOR)) {
                if (!StringUtils.isBlank(emailAddress)) {
                    final String dobEmail = StringUtils.toLowerCase(StringUtils.concatenate(dob, emailAddress));

                    this.updateCustomerIdMapBasedOnCount(customerID, dobEmail, dobEmailMap);
                }
            }

        }
    }

    /**
     * Sets the by email.
     *
     * @param email the email
     * @param customerID the customer ID
     */
    public void setByEmail(final String email, final String customerID) {

        if (!StringUtils.isEmpty(email) && !StringUtils.isEmpty(customerID)) {
            if (null == emailIDMap) {
                emailIDMap = new HashMap<>();
            }
            for (final String emailAddress : email.split(SEPRATOR)) {
                if (!StringUtils.isBlank(emailAddress)) {
                    this.updateCustomerIdMapBasedOnCount(customerID, emailAddress, emailIDMap);
                }
            }

        }
    }

    /**
     * Gets the by email.
     *
     * @param email the email
     * @return the by email
     */
    public String getByEmail(final String email) {

        String customerId = null;
        if (!StringUtils.isEmpty(email) && null != emailIDMap) {
            final String[] split = email.split(SEPRATOR);
            for (final String emailAddress : split) {
                if (!StringUtils.isBlank(emailAddress)) {
                    final PNRSCustomerID pnrsCustomerID = emailIDMap.get(emailAddress);
                    if (null != pnrsCustomerID) {
                        customerId = pnrsCustomerID.getCustomerID();
                        break;
                    }
                }
            }
        }

        return customerId;
    }

    /**
     * Sets the by DOB.
     *
     * @param dob the dob
     * @param customerID the customer ID
     */
    public void setByDOB(final String dob, final String customerID) {

        if (!StringUtils.isEmpty(dob) && !StringUtils.isEmpty(customerID)) {

            if (null == dobMap) {
                dobMap = new HashMap<>();
            }

            this.updateCustomerIdMapBasedOnCount(customerID, dob, dobMap);
        }
    }

    /**
     * Gets the by DOB.
     *
     * @param dob the dob
     * @return the by DOB
     */
    public String getByDOB(final String dob) {

        return dobMap == null ? null : null != dobMap.get(dob) ? dobMap.get(dob).getCustomerID() : null;
    }

    /**
     * Gets the by single field.
     *
     * @param fields the fields
     * @return the by single field
     */
    public String getBySingleField(final String... fields) {

        PNRSCustomerID pnrsCustomerID = null;
        String customerID = null;
        if (null != onlyOneFieldPresentMap) {
            if (!ArrayUtils.isEmpty(fields)) {
                int count = 0;
                String finalField = null;
                for (final String field : fields) {
                    if (!StringUtils.isEmpty(field)) {
                        finalField = field;
                        count++;
                    }
                }
                if (count == 1) {
                    for (final String fieldSplitedValue : finalField.split(SEPRATOR)) {
                        if (!StringUtils.isBlank(fieldSplitedValue)) {
                            pnrsCustomerID = onlyOneFieldPresentMap.get(fieldSplitedValue);
                            if (null != pnrsCustomerID) {
                                customerID = pnrsCustomerID.getCustomerID();
                                break;
                            }
                        }
                    }
                }
            }
        }
        return customerID;
    }

    public String getByTwoField(final String field1, final String field2) {

        PNRSCustomerID pnrsCustomerID = null;
        String customerID = null;
        if (null != onlyTwoFieldPresentMap) {
            if (!StringUtils.isBlank(field1) && !StringUtils.isBlank(field2)) {
                for (final String f1 : field1.split(SEPRATOR)) {
                    for (final String f2 : field2.split(SEPRATOR)) {
                        if (!StringUtils.isBlank(f1) && !StringUtils.isBlank(f2)) {
                            final String id = StringUtils.toLowerCase(StringUtils.concatenate(f1, f2));
                            pnrsCustomerID = onlyTwoFieldPresentMap.get(id);
                            if (null != pnrsCustomerID) {
                                customerID = pnrsCustomerID.getCustomerID();
                                break;
                            }
                        }
                    }
                    if (null != customerID) {
                        break;
                    }
                }
            }
        }
        return customerID;
    }

    /**
     * Sets the by single field.
     *
     * @param customerID the customer ID
     * @param fields the fields
     */
    public void setBySingleField(final String customerID, final String... fields) {

        if (null == onlyOneFieldPresentMap) {
            onlyOneFieldPresentMap = new HashMap<>();
        }
        if (!ArrayUtils.isEmpty(fields)) {
            for (final String field : fields) {
                if (!StringUtils.isEmpty(field)) {
                    for (final String fieldSplitedValue : field.split(SEPRATOR)) {
                        if (!StringUtils.isBlank(fieldSplitedValue)) {
                            this.updateCustomerIdMapBasedOnCount(customerID, fieldSplitedValue, onlyOneFieldPresentMap);
                        }
                    }

                }
            }
        }
    }

    public void setByTwoField(final String customerID, final String field1, final String field2) {

        if (null == onlyTwoFieldPresentMap) {
            onlyTwoFieldPresentMap = new HashMap<>();
        }
        if (!StringUtils.isBlank(field1) && !StringUtils.isBlank(field2)) {
            for (final String f1 : field1.split(SEPRATOR)) {
                for (final String f2 : field2.split(SEPRATOR)) {
                    if (!StringUtils.isBlank(f2) && !StringUtils.isEmpty(f1)) {
                        this.updateCustomerIdMapBasedOnCount(customerID, StringUtils.toLowerCase(StringUtils.concatenate(f1, f2)),
                            onlyTwoFieldPresentMap);
                    }
                }
            }
        }
    }

    /**
     * Sets the by FT and doc number.
     *
     * @param ftNumber the ft number
     * @param docNumber the doc number
     * @param lastName the last name
     * @param customerID the customer ID
     */
    public void setByFTAndDocNumber(final String ftNumber, final String docNumber, final String lastName, final String customerID) {

        if (!StringUtils.isEmpty(ftNumber) && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(docNumber)) {
            if (null == docAndFTNumberMap) {
                docAndFTNumberMap = new HashMap<>();
            }
            for (final String ftNum : ftNumber.split(SEPRATOR)) {
                for (final String docNum : docNumber.split(SEPRATOR)) {
                    if (!StringUtils.isBlank(ftNum) && !StringUtils.isBlank(docNum)) {
                        final String id = StringUtils.concatenate(lastName, ftNum, docNum);

                        this.updateCustomerIdMapBasedOnCount(customerID, id, docAndFTNumberMap);
                    }
                }
            }

        }
    }

    /**
     * Gets the by FT and doc number.
     *
     * @param ftNumber the ft number
     * @param docNumber the doc number
     * @param lastName the last name
     * @return the by FT and doc number
     */
    public String getByFTAndDocNumber(final String ftNumber, final String docNumber, final String lastName) {

        String customerID = null;
        PNRSCustomerID pnrsCustomerID = null;
        if (null != docAndFTNumberMap && !StringUtils.isEmpty(ftNumber) && !StringUtils.isEmpty(lastName)
            && !StringUtils.isEmpty(docNumber)) {
            for (final String ftNum : ftNumber.split(SEPRATOR)) {
                for (final String docNum : docNumber.split(SEPRATOR)) {
                    if (!StringUtils.isBlank(ftNum) && !StringUtils.isBlank(docNum)) {
                        final String id = StringUtils.concatenate(lastName, ftNum, docNum);
                        pnrsCustomerID = docAndFTNumberMap.get(id);
                        if (null != pnrsCustomerID) {
                            customerID = pnrsCustomerID.getCustomerID();
                            break;
                        }
                    }
                }
                if (null != customerID) {
                    break;
                }
            }
        }

        return customerID;
    }

    /**
     * Update customer id map based on count.
     *
     * @param customerID the customer ID
     * @param mapKey the map key
     * @param map the map
     */
    private void updateCustomerIdMapBasedOnCount(final String customerID,
                                                 final String mapKey,
                                                 final Map<String, PNRSCustomerID> map) {

        PNRSCustomerID value = map.get(mapKey);
        if (value != null) {
            value = new PNRSCustomerID(2, customerID);
        } else {
            value = new PNRSCustomerID(1, customerID);
        }
        map.put(mapKey, value);
    }

    public void customerIDHistoryUpdateMap(Row row1, String customerIDUpdated) {

        String lastName = StringUtils.toLowerString(row1.getAs(CustomerIDConstants.LAST_NAME_COLUMN));
        String dob = StringUtils.toLowerString(row1.getAs(CustomerIDConstants.DOB_COLUMN));
        String docNbr = StringUtils.toLowerString(row1.getAs(CustomerIDConstants.DOC_NUMBER_COLUMN));
        String email = StringUtils.toLowerString(row1.getAs(CustomerIDConstants.EMAIL_COLUMN));
        String ftNbr = StringUtils.toLowerString(row1.getAs(CustomerIDConstants.TEMP_FREQUENT_TRAVELER_NBR_COLUMN));

        // Update last name dob map
        if (!StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(dob) && null != lastNameDOBMap) {
            final String lastNameDOB = StringUtils.toLowerCase(StringUtils.concatenate(lastName, dob));
            this.updateCustomerIdMap(customerIDUpdated, lastNameDOB, lastNameDOBMap);
        }
        // update lastNameDocNbr Map
        if (!StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(docNbr) && null != lastNameDocNBRMap) {
            for (final String docNum : docNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(docNum)) {
                    final String lastNameDocNBR = StringUtils.toLowerCase(StringUtils.concatenate(lastName, docNum));
                    this.updateCustomerIdMap(customerIDUpdated, lastNameDocNBR, lastNameDocNBRMap);
                }
            }
        }
        // update lastNameFTNbr Map
        if (null != lastNameFTNbrMap && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(ftNbr)) {
            for (final String ftNum : ftNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(ftNum)) {
                    final String lastNameFTNbr = StringUtils.toLowerCase(StringUtils.concatenate(lastName, ftNum));
                    this.updateCustomerIdMap(customerIDUpdated, lastNameFTNbr, lastNameFTNbrMap);
                }
            }
        }
        // update lastNameEmailMap
        if (null != lastNameEmailMap && !StringUtils.isEmpty(lastName) && !StringUtils.isEmpty(email)) {
            for (final String emailAddress : email.split(SEPRATOR)) {
                if (!StringUtils.isBlank(emailAddress)) {
                    final String lastNameEmail = StringUtils.toLowerCase(StringUtils.concatenate(lastName, emailAddress));
                    this.updateCustomerIdMap(customerIDUpdated, lastNameEmail, lastNameEmailMap);
                }
            }
        }
        // update dobDocNBRMap
        if (null != dobDocNBRMap && !StringUtils.isEmpty(dob) && !StringUtils.isEmpty(docNbr)) {
            for (final String docNum : docNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(docNum)) {
                    final String dobDocNBR = StringUtils.toLowerCase(StringUtils.concatenate(dob, docNum));
                    this.updateCustomerIdMap(customerIDUpdated, dobDocNBR, dobDocNBRMap);
                }
            }
        }
        // update dobFTNbrMap
        if (null != dobFTNbrMap && !StringUtils.isEmpty(dob) && !StringUtils.isEmpty(ftNbr)) {
            for (final String ftNum : ftNbr.split(SEPRATOR)) {
                if (!StringUtils.isBlank(ftNum)) {
                    final String dobFTNbr = StringUtils.toLowerCase(StringUtils.concatenate(dob, ftNum));
                    this.updateCustomerIdMap(customerIDUpdated, dobFTNbr, dobFTNbrMap);
                }
            }
        }
        // update dobEmailMap
        if (null != dobEmailMap && !StringUtils.isEmpty(dob) && !StringUtils.isEmpty(email)) {
            for (final String emailAddress : email.split(SEPRATOR)) {
                if (!StringUtils.isBlank(emailAddress)) {
                    final String dobEmail = StringUtils.toLowerCase(StringUtils.concatenate(dob, emailAddress));
                    this.updateCustomerIdMap(customerIDUpdated, dobEmail, dobEmailMap);
                }
            }
        }
        // update emailIDMap
        if (null != emailIDMap && !StringUtils.isEmpty(email)) {
            for (final String emailAddress : email.split(SEPRATOR)) {
                if (!StringUtils.isBlank(emailAddress)) {
                    this.updateCustomerIdMap(customerIDUpdated, emailAddress, emailIDMap);
                }
            }

        }
        // update dobMap
        if (null != dobMap && !StringUtils.isEmpty(dob)) {
            this.updateCustomerIdMap(customerIDUpdated, dob, dobMap);
        }
        // update docAndFTNumberMap
        if (null != docAndFTNumberMap && !StringUtils.isEmpty(ftNbr) && !StringUtils.isEmpty(lastName)
            && !StringUtils.isEmpty(docNbr)) {
            for (final String ftNum : ftNbr.split(SEPRATOR)) {
                for (final String docNum : docNbr.split(SEPRATOR)) {
                    if (!StringUtils.isBlank(ftNum) && !StringUtils.isBlank(docNum)) {
                        final String id = StringUtils.concatenate(lastName, ftNum, docNum);
                        this.updateCustomerIdMap(customerIDUpdated, id, docAndFTNumberMap);
                    }
                }
            }
        }
        // update onlyOneFieldPresentMap
        String[] fields = { ftNbr, docNbr, email, dob };
        if (null != onlyOneFieldPresentMap && !ArrayUtils.isEmpty(fields)) {
            for (final String field : fields) {
                if (!StringUtils.isEmpty(field)) {
                    for (final String fieldSplitedValue : field.split(SEPRATOR)) {
                        if (!StringUtils.isBlank(fieldSplitedValue)) {
                            this.updateCustomerIdMap(customerIDUpdated, fieldSplitedValue, onlyOneFieldPresentMap);
                        }
                    }

                }
            }
        }

    }

    private void updateCustomerIdMap(final String customerID, final String mapKey, final Map<String, PNRSCustomerID> map) {

        PNRSCustomerID value = map.get(mapKey);
        if (null != value) {
            value.setCustomerID(customerID);
            map.put(mapKey, value);
        }

    }

    /**
     * The Class PNRSCustomerID.
     */
    private static class PNRSCustomerID implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /** The count. */
        int count;

        /** The customer ID. */
        String customerID;

        /**
         * Instantiates a new PNRS customer ID.
         *
         * @param count the count
         * @param customerID the customer ID
         */
        public PNRSCustomerID(final int count, final String customerID) {

            super();
            this.count = count;
            this.customerID = customerID;
        }

        /**
         * Gets the count.
         *
         * @return the count
         */
        public int getCount() {

            return count;
        }

        /**
         * Sets the count.
         *
         * @param count the new count
         */
        public void setCount(final int count) {

            this.count = count;
        }

        /**
         * Gets the customer ID.
         *
         * @return the customer ID
         */
        public String getCustomerID() {

            return customerID;
        }

        /**
         * Sets the customer ID.
         *
         * @param customerID the new customer ID
         */
        public void setCustomerID(final String customerID) {

            this.customerID = customerID;
        }

    }
}
