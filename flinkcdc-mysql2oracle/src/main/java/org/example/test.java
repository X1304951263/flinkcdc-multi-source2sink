package org.example;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @创建人 君子固穷
 * @创建时间 2024-03-22
 * @描述
 */
public class test {



        public static void main(String[] args) {
            StringBuilder sb = new StringBuilder();
            sb.append("target.\"LASTVERNO\",\n" +
                    "        target.\"TZBVERID\",\n" +
                    "        target.\"INCHARGEID\",\n" +
                    "        target.\"ISHISTORY\",\n" +
                    "        target.\"ISLAST\",\n" +
                    "        target.\"VERNO\",\n" +
                    "        target.\"REMARK\",\n" +
                    "        target.\"COMPANYNAME\",\n" +
                    "        target.\"DELETERID\",\n" +
                    "        target.\"CITYID\",\n" +
                    "        target.\"INCHARGETIME\",\n" +
                    "        target.\"BASECOSTID\",\n" +
                    "        target.\"PRODUCTLINEID\",\n" +
                    "        target.\"RECORDSTATUS\",\n" +
                    "        target.\"LASTMODIFICATIONTIME\",\n" +
                    "        target.\"ISDELETED\",\n" +
                    "        target.\"MDMDESIGNINDEXID\",\n" +
                    "        target.\"ISDYNAMICCOST\",\n" +
                    "        target.\"COMPANYID\",\n" +
                    "        target.\"PROJECTLAND\",\n" +
                    "        target.\"LASTVERID\",\n" +
                    "        target.\"DISTRICTID\",\n" +
                    "        target.\"ISSUPPLEMENT\",\n" +
                    "        target.\"CREATORID\",\n" +
                    "        target.\"DELETIONTIME\",\n" +
                    "        target.\"BATCHID\",\n" +
                    "        target.\"PRODUCTLINENAME\",\n" +
                    "        target.\"PROJECTID\",\n" +
                    "        target.\"CITYNAME\",\n" +
                    "        target.\"LASTMODIFIERID\",\n" +
                    "        target.\"CALCULATIONRANGE\",\n" +
                    "        target.\"DISTRICTNAME\",\n" +
                    "        target.\"BGU\",\n" +
                    "        target.\"TENANTID\",\n" +
                    "        target.\"BUDGETSTAGE\",\n" +
                    "        target.\"CALCULATIONTYPE\",\n" +
                    "        target.\"CREATIONTIME\",\n" +
                    "        target.\"PRODUCTCATEGORY\",\n" +
                    "        target.\"ID\",\n" +
                    "        target.\"ISCURRENTSTAGELASTVER\",\n" +
                    "        target.\"PROJECTCOMPANY\",\n" +
                    "        target.\"PROJECTLANDID\",\n" +
                    "        target.\"BASECOSTNO\",\n" +
                    "        target.\"EFFECTIVEDATE\",\n" +
                    "\n" +
                    "        target.\"LASTMODIFICATIONTIME\",\n" +
                    "        target.\"SEQNO\",\n" +
                    "        target.\"ISDELETED\",\n" +
                    "        target.\"CREATORID\",\n" +
                    "        target.\"DELETIONTIME\",\n" +
                    "        target.\"PRODUCTLINENAME\",\n" +
                    "        target.\"REMARK\",\n" +
                    "        target.\"LASTMODIFIERID\",\n" +
                    "        target.\"BGU\",\n" +
                    "        target.\"DELETERID\",\n" +
                    "        target.\"TENANTID\",\n" +
                    "        target.\"PRODUCTLINECODE\",\n" +
                    "        target.\"CREATIONTIME\",\n" +
                    "        target.\"PRODUCTCATEGORY\",\n" +
                    "        target.\"ID\",\n" +
                    "        target.\"RECORDSTATUS\",\n" +
                    "\n" +
                    "        target.\"SALESSPECIALAMT\",\n" +
                    "        target.\"TZBVERID\",\n" +
                    "        target.\"BALANCEAMT\",\n" +
                    "        target.\"PRODUCTNAME\",\n" +
                    "        target.\"PROJECTNAME\",\n" +
                    "        target.\"HARDCOVERAMT\",\n" +
                    "        target.\"BUILDINSPECIALAMT\",\n" +
                    "        target.\"TZBTARGETCOSTAMT\",\n" +
                    "        target.\"BUILDINNOTHARDCOVERAMT\",\n" +
                    "        target.\"COSTCALCVERID\",\n" +
                    "        target.\"BUILDINHARDCOVERAMT\",\n" +
                    "        target.\"DELETERID\",\n" +
                    "        target.\"RECESSIVEAMT\",\n" +
                    "        target.\"SALESHARDCOVERAMT\",\n" +
                    "        target.\"LASTFIVECOSTAMT\",\n" +
                    "        target.\"DOMINANCEAMT\",\n" +
                    "        target.\"SIGNAMT\",\n" +
                    "        target.\"PRODUCTLINEID\",\n" +
                    "        target.\"TOTALLANDAREA\",\n" +
                    "        target.\"LASTMODIFICATIONTIME\",\n" +
                    "        target.\"ISDELETED\",\n" +
                    "        target.\"COMPANYID\",\n" +
                    "        target.\"LASTVERID\",\n" +
                    "        target.\"TARGETCOSTAMT\",\n" +
                    "        target.\"CREATORID\",\n" +
                    "        target.\"SPECIALCOSTAMT\",\n" +
                    "        target.\"LASTTARGETCOSTAMT\",\n" +
                    "        target.\"DELETIONTIME\",\n" +
                    "        target.\"PRODUCTLINENAME\",\n" +
                    "        target.\"PROJECTID\",\n" +
                    "        target.\"BUILDINGUNITPRICE\",\n" +
                    "        target.\"TOTALSALEAREA\",\n" +
                    "        target.\"SALEPERCENT\",\n" +
                    "        target.\"PROJECTMANAGER\",\n" +
                    "        target.\"LASTMODIFIERID\",\n" +
                    "        target.\"TENANTID\",\n" +
                    "        target.\"PAYAMT\",\n" +
                    "        target.\"SALESNOTHARDCOVERAMT\",\n" +
                    "        target.\"TZBFIVECOSTAMT\",\n" +
                    "        target.\"FIVECOSTAMT\",\n" +
                    "        target.\"FOURCOSTAMT\",\n" +
                    "        target.\"SALESUNITPRICE\",\n" +
                    "        target.\"TOTALBUILDAREA\",\n" +
                    "        target.\"CREATIONTIME\",\n" +
                    "        target.\"NONCONTRACTAMT\",\n" +
                    "        target.\"ID\",\n" +
                    "        target.\"FIVEDYNAMICCOSTAMT\",\n" +
                    "        target.\"CONTINGENCYAMT\",");

            sb.delete(sb.length() - 1, sb.length());
        }
}
