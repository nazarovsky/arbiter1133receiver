# receive UDP measurements from Arbiter 1133A
# store them in CSV files
# Nazarovsky Alexander, 2014-2015
import signal
import socket
import struct
from math import pi
from datetime import datetime,date
import time

def rad2deg(ang1):
    return ang1*180/pi

# return angle difference in degrees (!)
# note that angle is between I and U (notation - between 2nd term and 1st term) \phi_{UI}=\phi_{U}-\phi_{I}
#    terms: 2nd  1st  so that \phi_{UI} = dangle(I,U)
def dangle(ang1,ang2):
    diffang=ang2-ang1
    if diffang<=-pi:
        diffang+=2*pi
    if diffang>pi:
        diffang-=2*pi
    return rad2deg(diffang)

UDP_IP = "192.168.0.17" # IP and Port of the Arbiter 1133A
UDP_PORT = 17000        # Note that we need to enable 

sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
sock.bind((UDP_IP, UDP_PORT))

f0 = open("results.csv", 'w')
f0.write("Time, f, Ua, Uab, Ia, KUa, Phi_Uab, Phi_Iab, Phi_UIa, Pa, Qa, Sa, Ub, Ubc, Ib, KUb, Phi_Ubc, Phi_Ibc, Phi_UIb, Pb, Qb, Sb, Uc, Uca, Ic, KUc, Phi_Uca, Phi_Ica, Phi_UIc, Pa, Qa, Sa, K2U, K0U, K2I, K0I, P, Q, S\n");

f1 = open("results_bas.csv", 'w')
f1.write("Time, f, Ua, Ub, Uc, Ia, Ib, Ic, PhiUIa, PhiUIb, PhiUIc, PhiUab, PhiUbc, PhiUca, PhiIab, PhiIbc, PhiIca \n");

f2 = open("results_pwr.csv", 'w')
f2.write("Time, PA, QA, SA, PFA, QQA, PB, QB, SB, PFB, QQB, PC, QC, SC, PFC, QQC, PTOT, QTOT, STOT, PFTOT, QQTOT \n");

f3 = open("results_seq.csv", 'w')
f3.write("Time, U0MAG, U1MAG,U2MAG, I0MAG,I1MAG,I2MAG, PhiUI0, PhiUI1, PhiUI2 \n");

f4 = open("results_pst.csv", 'w')
f4.write("Time, PstUa, PstUb, PstUc, PstIa, PstIb, PstIc\n");

f5 = open("results_thd.csv", 'w')
f5.write("Time, ThdUa, ThdUb, ThdUc, ThdIa, ThdIb, ThdIc\n");

f6 = open("results_har.csv", 'w')
f6.write("Time,")
for k in range (1,51):
    f6.write("Ua({0}),".format(k))
for k in range (1,51):
    f6.write("Ub({0}),".format(k))
for k in range (1,51):
    f6.write("Uc({0}),".format(k))
for k in range (1,51):
    f6.write("Ia({0}),".format(k))
for k in range (1,51):
    f6.write("Ib({0}),".format(k))
for k in range (1,50):
    f6.write("Ic({0}),".format(k))
f6.write("Ic(50)\n")

f7 = open("results_cal.csv", 'w')
f7.write("Time, Ua, Ub, Uc, Ia, Ib, Ic, PhiUIa, PhiUIb, PhiUIc \n");

while True:
    data, addr = sock.recvfrom(1500) # buffer size is 1500 bytes
    packet = data
#    print "Got a packet len={0}".format(len(packet))
    # type of packets is determined from their size. sorry
    
    # if packet is a Basic Measurements type 
    if (len(packet)==244):
    # general
        time1 = datetime.fromtimestamp(struct.unpack('I',packet[6:10])[0] & 0x0FFFFFFF)
    # time1 = datetime.fromtimestamp(0x81842ce4)
        time1 = time1.replace(year=date.today().year)   # because Arbiter always sends timestamps with Year=1970 we need to determine current year from PC clock

        ua = struct.unpack('<f',packet[22:26])[0]
        phiua = struct.unpack('<f',packet[26:30])[0]
        uc = struct.unpack('<f',packet[30:34])[0]
        phiuc = struct.unpack('<f',packet[34:38])[0]
        ub = struct.unpack('<f',packet[38:42])[0]
        phiub = struct.unpack('<f',packet[42:46])[0]

        ia = struct.unpack('<f',packet[46:50])[0]
        phiia = struct.unpack('<f',packet[50:54])[0]
        ic = struct.unpack('<f',packet[54:58])[0]
        phiic = struct.unpack('<f',packet[58:62])[0]
        ib = struct.unpack('<f',packet[62:66])[0]
        phiib = struct.unpack('<f',packet[66:70])[0]
    # sequences
        u0 = struct.unpack('<f',packet[70:74])[0]
        phiu0 = struct.unpack('<f',packet[74:78])[0]
        u1 = struct.unpack('<f',packet[78:82])[0]
        phiu1 = struct.unpack('<f',packet[82:86])[0]
        u2 = struct.unpack('<f',packet[86:90])[0]
        phiu2 = struct.unpack('<f',packet[90:94])[0]

        i0 = struct.unpack('<f',packet[94:98])[0]
        phii0 = struct.unpack('<f',packet[98:102])[0]
        i1 = struct.unpack('<f',packet[102:106])[0]
        phii1 = struct.unpack('<f',packet[106:110])[0]
        i2 = struct.unpack('<f',packet[110:114])[0]
        phii2 = struct.unpack('<f',packet[114:118])[0]
    # powers    
        PA = struct.unpack('<f',packet[118:122])[0]
        QA = struct.unpack('<f',packet[122:126])[0]
        SA = struct.unpack('<f',packet[126:130])[0]
        PFA = struct.unpack('<f',packet[130:134])[0]
        QQA = struct.unpack('<f',packet[134:138])[0]

        PC = struct.unpack('<f',packet[138:142])[0]
        QC = struct.unpack('<f',packet[142:146])[0]
        SC = struct.unpack('<f',packet[146:150])[0]
        PFC = struct.unpack('<f',packet[150:154])[0]
        QQC = struct.unpack('<f',packet[154:158])[0]

        PB = struct.unpack('<f',packet[158:162])[0]
        QB = struct.unpack('<f',packet[162:166])[0]
        SB = struct.unpack('<f',packet[166:170])[0]
        PFB = struct.unpack('<f',packet[170:174])[0]
        QQB = struct.unpack('<f',packet[174:178])[0]

        PTOT = struct.unpack('<f',packet[178:182])[0]
        QTOT = struct.unpack('<f',packet[182:186])[0]
        STOT = struct.unpack('<f',packet[186:190])[0]
        PFTOT = struct.unpack('<f',packet[190:194])[0]
        QQTOT = struct.unpack('<f',packet[194:198])[0]
    # frequency
        f = struct.unpack('<f',packet[198:202])[0]
        df= struct.unpack('<f',packet[202:206])[0]
        ROCOF= struct.unpack('<f',packet[206:210])[0]    
        tdevsec= struct.unpack('<f',packet[210:214])[0]
        tdevcyc= struct.unpack('<f',packet[214:218])[0]    
    # flicker
        PstUa= struct.unpack('<f',packet[218:222])[0]
        PstIa= struct.unpack('<f',packet[222:226])[0]
        PstUc= struct.unpack('<f',packet[226:230])[0]
        PstIc= struct.unpack('<f',packet[230:234])[0]
        PstUb= struct.unpack('<f',packet[234:238])[0]
        PstIb= struct.unpack('<f',packet[238:242])[0]

    # # CALCULATE UNBALANCE
        if (u1>0):
            K2U = u2/u1*100 
            K0U = u0/u1*100
        else:
            K2U=0
            K0U=0

        if (i1>0):
            K2I = i2/i1*100 
            K0I = i0/i1*100
        else:
            K2I=0
            K0I=0
            
        Uab=0
        Ubc=0
        Uca=0
    # if len(packet)==244 
    if (len(packet)==1472):   # Arbiter 1133A splits info about harmonics into two packets
        harm_packet=packet    # just store the 1st part of harmonics (it comes in two packets 1st=1472 2nd=1020
    # if len(packet)==1472 
    # get remaining harmonics from the second packet
    if (len(packet)==1020):
        packet=harm_packet+packet
#        print len(packet)
        Ua_h_mag=[]
        Ua_h_ang=[]
        Ia_h_mag=[]
        Ia_h_ang=[]
        Uc_h_mag=[]
        Uc_h_ang=[]
        Ic_h_mag=[]
        Ic_h_ang=[]
        Ub_h_mag=[]
        Ub_h_ang=[]
        Ib_h_mag=[]
        Ib_h_ang=[]
        for k in range (1,51):
            Ua_h_mag.append(struct.unpack('<f',packet[90+(k-1)*4*2:94+(k-1)*4*2])[0])
            Ua_h_ang.append(struct.unpack('<f',packet[94+(k-1)*4*2:98+(k-1)*4*2])[0])
            Ia_h_mag.append(struct.unpack('<f',packet[490+(k-1)*4*2:494+(k-1)*4*2])[0])
            Ia_h_ang.append(struct.unpack('<f',packet[494+(k-1)*4*2:498+(k-1)*4*2])[0])
            Uc_h_mag.append(struct.unpack('<f',packet[890+(k-1)*4*2:894+(k-1)*4*2])[0])
            Uc_h_ang.append(struct.unpack('<f',packet[894+(k-1)*4*2:898+(k-1)*4*2])[0])
            Ic_h_mag.append(struct.unpack('<f',packet[1290+(k-1)*4*2:1294+(k-1)*4*2])[0])
            Ic_h_ang.append(struct.unpack('<f',packet[1294+(k-1)*4*2:1298+(k-1)*4*2])[0])
            Ub_h_mag.append(struct.unpack('<f',packet[1690+(k-1)*4*2:1694+(k-1)*4*2])[0])
            Ub_h_ang.append(struct.unpack('<f',packet[1694+(k-1)*4*2:1698+(k-1)*4*2])[0])
            Ib_h_mag.append(struct.unpack('<f',packet[2090+(k-1)*4*2:2094+(k-1)*4*2])[0])
            Ib_h_ang.append(struct.unpack('<f',packet[2094+(k-1)*4*2:2098+(k-1)*4*2])[0])
            
            Ua_h_ang[k-1]=rad2deg(Ua_h_ang[k-1])
            Ia_h_ang[k-1]=rad2deg(Ia_h_ang[k-1])
            Uc_h_ang[k-1]=rad2deg(Uc_h_ang[k-1])
            Ic_h_ang[k-1]=rad2deg(Ic_h_ang[k-1])
            Ub_h_ang[k-1]=rad2deg(Ub_h_ang[k-1])
            Ib_h_ang[k-1]=rad2deg(Ib_h_ang[k-1])    
        
            
        #print "#>{0} {1} {2} {3} {4}".format(Ic_h_mag[0],Ic_h_mag[1],Ic_h_mag[2], Ic_h_mag[3],Ic_h_mag[4])    
        #print "#>{0} {1} {2} {3} {4}".format(Ic_h_ang[0],Ic_h_ang[1],Ic_h_ang[2], Ic_h_ang[3],Ic_h_ang[4])    
        #print "#>{0} {1} {2} {3} {4}".format(Ia_h_mag[0],Ia_h_mag[1],Ia_h_mag[2], Ia_h_mag[3],Ia_h_mag[4])    
        #print "#>{0} {1} {2} {3} {4}".format(Ia_h_ang[0],Ia_h_ang[1],Ia_h_ang[2], Ia_h_ang[3],Ia_h_ang[4])    
        #print "#>{0} {1} {2} {3} {4}".format(Ub_h_mag[0],Ub_h_mag[1],Ub_h_mag[2], Ub_h_mag[3],Ub_h_mag[4])    
        #print "#>{0} {1} {2} {3} {4}".format(Ub_h_ang[0],Ub_h_ang[1],Ub_h_ang[2], Ub_h_ang[3],Ub_h_ang[4])    
        #print "#>{0} {1} {2} {3} {4}".format(Ib_h_mag[0],Ib_h_mag[1],Ib_h_mag[2], Ib_h_mag[3],Ib_h_mag[4])    
        #print "#>{0} {1} {2} {3} {4}".format(Ib_h_ang[0],Ib_h_ang[1],Ib_h_ang[2], Ib_h_ang[3],Ib_h_ang[4])    
    # if (len(packet)==1020):        
    
    
    if (len(packet)==140):    
        time2 = datetime.fromtimestamp(  struct.unpack('I',packet[6:10])[0] & 0x0FFFFFFF)
        time2 = time2.replace(year=date.today().year)   # because Arbiter sends Year=1970.
        
        UaRMSTHD = struct.unpack('<f',packet[18:22])[0]
        UaRMSK = struct.unpack('<f',packet[22:26])[0]
        UaTHDF = struct.unpack('<f',packet[26:30])[0]
        UaTHDT = struct.unpack('<f',packet[30:34])[0]
        UaKFACTOR = struct.unpack('<f',packet[34:38])[0]
        
        IaRMSTHD = struct.unpack('<f',packet[38:42])[0]
        IaRMSK = struct.unpack('<f',packet[42:46])[0]
        IaTHDF = struct.unpack('<f',packet[46:50])[0]
        IaTHDT = struct.unpack('<f',packet[50:54])[0]
        IaKFACTOR = struct.unpack('<f',packet[54:58])[0]        

        UcRMSTHD = struct.unpack('<f',packet[58:62])[0]
        UcRMSK = struct.unpack('<f',packet[62:66])[0]
        UcTHDF = struct.unpack('<f',packet[66:70])[0]
        UcTHDT = struct.unpack('<f',packet[70:74])[0]
        UcKFACTOR = struct.unpack('<f',packet[74:78])[0]
        
        IcRMSTHD = struct.unpack('<f',packet[78:82])[0]
        IcRMSK = struct.unpack('<f',packet[82:86])[0]
        IcTHDF = struct.unpack('<f',packet[86:90])[0]
        IcTHDT = struct.unpack('<f',packet[90:94])[0]
        IcKFACTOR = struct.unpack('<f',packet[94:98])[0]        

        UbRMSTHD = struct.unpack('<f',packet[98:102])[0]
        UbRMSK = struct.unpack('<f',packet[102:106])[0]
        UbTHDF = struct.unpack('<f',packet[106:110])[0]
        UbTHDT = struct.unpack('<f',packet[110:114])[0]
        UbKFACTOR = struct.unpack('<f',packet[114:118])[0]
        
        IbRMSTHD = struct.unpack('<f',packet[118:122])[0]
        IbRMSK = struct.unpack('<f',packet[122:126])[0]
        IbTHDF = struct.unpack('<f',packet[126:130])[0]
        IbTHDT = struct.unpack('<f',packet[130:134])[0]
        IbKFACTOR = struct.unpack('<f',packet[134:138])[0]        

        
        UcTHDF = struct.unpack('<f',packet[66:70])[0]
        IcTHDF = struct.unpack('<f',packet[86:90])[0]
        UbTHDF = struct.unpack('<f',packet[106:110])[0]
        IbTHDF = struct.unpack('<f',packet[126:130])[0]
        
        line=ua+ub+uc
        # print some measurements to console
        print ">{0} {1} {2} {3} {4}".format(time1, f,ua,ub,uc)

        f0.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},{38}\n".format(\
        time1, f, ua, Uab, ia, UaTHDF, dangle(phiub,phiua), dangle(phiib,phiia), dangle(phiia,phiua),PA,QA,SA, \
        ub, Ubc, ib, UbTHDF, dangle(phiuc,phiub), dangle(phiic,phiib), dangle(phiib,phiub),PB,QB,SB, \
        uc, Uca, ic, UcTHDF, dangle(phiua,phiuc), dangle(phiia,phiic), dangle(phiic,phiuc),PC,QC,SC, \
        K2U, K0U, K2I, K0I, PTOT, QTOT, STOT))

        f1.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16}\n".format(\
        time1, f, ua,ub,uc,ia,ib,ic,dangle(phiia,phiua),dangle(phiib,phiub),dangle(phiic,phiuc),dangle(phiub,phiua),dangle(phiuc,phiub),dangle(phiua,phiuc),dangle(phiib,phiia),dangle(phiic,phiib),dangle(phiia,phiic)))

        f2.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20}\n".format(time1, PA,QA,SA,PFA,QQA,PB,QB,SB,PFB,QQB,PC,QC,SC,PFC,QQC,PTOT,QTOT,STOT,PFTOT,QQTOT))

        f3.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}\n".format(time1, u0,u1,u2,i0,i1,i2,dangle(phiu0,phii0),dangle(phiu1,phii1),dangle(phiu2,phii2)))

        f4.write("{0},{1},{2},{3},{4},{5},{6}\n".format(time1, PstUa,PstUb,PstUc, PstIa,PstIb, PstIc))
        
        f5.write("{0},{1},{2},{3},{4},{5},{6}\n".format(\
        time2, UaTHDF, UbTHDF, UcTHDF, IaTHDF, IbTHDF, IcTHDF))
        
        
        f6.write("{0},".format(time1))
        for k in range (1,51):
            f6.write("{0},".format(Ua_h_mag[k-1]))
        for k in range (1,51):
            f6.write("{0},".format(Ub_h_mag[k-1]))
        for k in range (1,51):
            f6.write("{0},".format(Uc_h_mag[k-1]))
        for k in range (1,51):
            f6.write("{0},".format(Ia_h_mag[k-1]))
        for k in range (1,51):
            f6.write("{0},".format(Ib_h_mag[k-1]))
        for k in range (1,50):
            f6.write("{0},".format(Ic_h_mag[k-1]))
        f6.write("{0}\n".format(Ic_h_mag[49]))
  
        f7.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}\n".format(\
        time1, ua, ub, uc, ia, ib, ic, dangle(phiia,phiua),dangle(phiib,phiub),dangle(phiic,phiuc)))
  
        
    # if len(packet)==140 
