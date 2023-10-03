import functools
from decimal import Decimal, getcontext

class Allocation(object) :
    def __init__(self, *args : list, **kwargs ) -> None:
        
        """    
        1 = begin cap , 2 = cost , 3 =adjusted cost
        r = {'rule' : 1, 'rule' : 2 , 'rule' : 3}
        """ 

        getcontext().prec = 16
        
        self.emp = args[0]
        self.tranche = args[1]
        self.begincap = args[2]
        self.opcontrib = args[3]
        self.opwithdraw = args[4]
        self.pl = args[5]
        self.eprule = kwargs['rule']
        

    def __adjcap(self) -> list:

        def adjcap(a,b,c):

            return a + b + c
        
        return list(map(adjcap, self.begincap, self.opcontrib, self.opwithdraw))
    

    def __epbase(self) -> list:

        def epbase(a = 0, b = 0, c =0 ):
            if self.eprule == 1 :
               f = a     
            if self.eprule == 2 :
               f = b + c 
            if self.eprule == 3 :
               f  = a + b + c
            return f
        
        return list(map(epbase, self.begincap, self.opcontrib, self.opwithdraw ))

    def allocate(self) -> list :
        
        def calc_ep() -> list:
                    
            def reduce(amtlist : list) -> Decimal:
                return  functools.reduce(lambda a , b : a+b , amtlist)
            
            def div(a : Decimal) :
                tot = reduce(amtlist= self.__epbase())
                if tot == 0:
                   d = 0
                else :    
                   d = round(float(a),15)/float(tot)
                return round(float(d),15)
             
            y = list(map(div, self.__epbase()))
            return y    

        def gav(ep, bkincm) :
            return ep + bkincm
        
        def mapping_perbucket(ep : Decimal) :
                        
            r  = Decimal(ep) * Decimal(i)
            return round(float(r),8)

        def mapping(ep : Decimal)  :
            
            pl = functools.reduce(lambda x , y : x+y , self.pl)    

            r  = Decimal(ep) * Decimal(pl)
            return round(float(r),8)
        
        ep = calc_ep()
        
        ep_tot =   functools.reduce(lambda a , b : Decimal(a) + Decimal(b), ep)
        
        if ep_tot > 1.00 :
           diff = abs(Decimal(1.00) - Decimal(ep_tot))
           ep[0] =  round(float(Decimal(ep[0])),10) - round(float(diff),10)
        elif ep_tot < 1.00 :
           diff = abs(Decimal(1.00) - Decimal(ep_tot))
           ep[0] =  round(float(Decimal(ep[0])),10) + round(float(diff),10)
        else :
           pass
        
        bookincm  = list(map(mapping, ep))
        
        gav_list = list(map(gav,self.__adjcap(),bookincm))
        

        bucket_alloc = []
        for i in self.pl :
            w = list(map(mapping_perbucket , ep))
            bucket_alloc.append(w)

       
        return list(zip(self.emp, self.tranche, self.begincap ,self.opcontrib,self.opwithdraw,self.__adjcap(), ep, bookincm,  *bucket_alloc, gav_list))
    
    
    
    


        